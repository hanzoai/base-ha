package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/litesql/go-ha"
)

// QuasarWriterProvider implements ha.LeaderProvider using Quasar-style
// deterministic writer-pinning over a heartbeat alive set.
//
// Quasar is leaderless — all nodes are equal validators. SQLite's single-writer
// constraint is satisfied by ranking alive peers by NodeID; the lowest-sorted
// gets the write lock. All others are replicas.
//
// O(peers) memory, O(1) per heartbeat. No RAFT, no snapshots.
type QuasarWriterProvider struct {
	cfg         QuasarWriterConfig
	localTarget string

	mu        sync.RWMutex
	alive     map[string]time.Time
	urls      map[string]string
	target    string
	closeCh   chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
	started   atomic.Bool
}

type QuasarWriterConfig struct {
	NodeID            string
	LocalTarget       string
	Peers             []string
	HeartbeatInterval time.Duration
	LeaseTimeout      time.Duration
}

func NewQuasarWriterProvider(cfg QuasarWriterConfig) (*QuasarWriterProvider, error) {
	if cfg.NodeID == "" {
		hn, _ := os.Hostname()
		cfg.NodeID = hn
	}
	if cfg.LocalTarget == "" {
		return nil, fmt.Errorf("LocalTarget is required")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 500 * time.Millisecond
	}
	if cfg.LeaseTimeout == 0 {
		cfg.LeaseTimeout = 3 * cfg.HeartbeatInterval
	}
	p := &QuasarWriterProvider{
		cfg:         cfg,
		localTarget: cfg.LocalTarget,
		alive:       map[string]time.Time{cfg.NodeID: time.Now()},
		urls:        map[string]string{cfg.NodeID: cfg.LocalTarget},
		closeCh:     make(chan struct{}),
		ready:       make(chan struct{}, 1),
	}
	p.start()
	return p, nil
}

func (p *QuasarWriterProvider) start() {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	go p.heartbeatLoop()
}

// go-ha calls these IsLeader/RedirectTarget/Ready — we implement that
// interface but the semantics are writer-pin, not leadership.
func (p *QuasarWriterProvider) IsLeader() bool         { return p.writerID() == p.cfg.NodeID }
func (p *QuasarWriterProvider) RedirectTarget() string { p.mu.RLock(); defer p.mu.RUnlock(); return p.target }
func (p *QuasarWriterProvider) Ready() chan struct{}    { return p.ready }
func (p *QuasarWriterProvider) Close()                 { close(p.closeCh) }

func (p *QuasarWriterProvider) writerID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	ids := make([]string, 0, len(p.alive))
	for id, last := range p.alive {
		if now.Sub(last) <= p.cfg.LeaseTimeout {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return p.cfg.NodeID
	}
	sort.Strings(ids)
	return ids[0]
}

func (p *QuasarWriterProvider) heartbeatLoop() {
	ticker := time.NewTicker(p.cfg.HeartbeatInterval)
	defer ticker.Stop()
	client := &http.Client{Timeout: p.cfg.HeartbeatInterval}
	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			p.mu.Lock()
			p.alive[p.cfg.NodeID] = time.Now()
			p.mu.Unlock()

			var wg sync.WaitGroup
			for _, peer := range p.cfg.Peers {
				wg.Add(1)
				go func(peer string) {
					defer wg.Done()
					p.sendHeartbeat(client, peer)
				}(peer)
			}
			wg.Wait()

			writerID := p.writerID()
			target := p.targetFor(writerID)
			p.mu.Lock()
			if target != p.target {
				slog.Info("ha: writer changed", "from", p.target, "to", target, "writer", writerID)
			}
			p.target = target
			p.mu.Unlock()
			p.readyOnce.Do(func() { close(p.ready) })
		}
	}
}

func (p *QuasarWriterProvider) targetFor(id string) string {
	if id == p.cfg.NodeID {
		return p.localTarget
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if u, ok := p.urls[id]; ok {
		return u
	}
	return p.localTarget
}

func (p *QuasarWriterProvider) sendHeartbeat(client *http.Client, peer string) {
	endpoint, err := url.JoinPath(peer, "/_ha/heartbeat")
	if err != nil {
		return
	}
	body, _ := json.Marshal(heartbeatMsg{NodeID: p.cfg.NodeID, Target: p.localTarget})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var reply heartbeatMsg
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&reply); err != nil {
		return
	}
	p.ingest(reply)
}

func (p *QuasarWriterProvider) ingest(msg heartbeatMsg) {
	if msg.NodeID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.alive[msg.NodeID] = time.Now()
	if msg.Target != "" {
		p.urls[msg.NodeID] = msg.Target
	}
}

func (p *QuasarWriterProvider) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var in heartbeatMsg
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p.ingest(in)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(heartbeatMsg{NodeID: p.cfg.NodeID, Target: p.localTarget})
}

type heartbeatMsg struct {
	NodeID string `json:"node_id"`
	Target string `json:"target"`
}

var _ ha.LeaderProvider = (*QuasarWriterProvider)(nil)
