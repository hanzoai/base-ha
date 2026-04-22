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

	mu           sync.RWMutex
	alive        map[string]time.Time
	urls         map[string]string
	target       string
	currentID    string // node-id of the currently elected writer (may be "" before first election)
	term         uint64 // monotonic; increments on every writer change
	leaseExpires time.Time
	closeCh      chan struct{}
	ready        chan struct{}
	readyOnce    sync.Once
	started      atomic.Bool
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
			if writerID != p.currentID {
				slog.Info("ha: writer changed",
					"from", p.currentID, "to", writerID,
					"target", target, "term", p.term+1)
				p.term++
				p.currentID = writerID
			}
			p.target = target
			// Lease expiry = last-seen of the writer + LeaseTimeout. If the
			// writer's last heartbeat is older than (now - LeaseTimeout) the
			// lease has already expired; writerID() will pick a new writer
			// on the next tick. We compute lease_expires from the writer's
			// last-seen so the gateway can detect staleness without clock skew.
			if last, ok := p.alive[writerID]; ok {
				p.leaseExpires = last.Add(p.cfg.LeaseTimeout)
			} else {
				p.leaseExpires = time.Now().Add(p.cfg.LeaseTimeout)
			}
			// Update the atomic snapshot used by the response-header
			// middleware. Must happen under the lock so term/url pair
			// is coherent.
			setWriterHeader(p.target, p.term)
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

// LeaderInfo satisfies HAEndpointProvider. Returns the current writer URL,
// writer node-id, term, and lease-expiry time. All reads are under a
// single RLock so term/url are coherent.
func (p *QuasarWriterProvider) LeaderInfo() (string, string, uint64, time.Time) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.target, p.currentID, p.term, p.leaseExpires
}

// Peers satisfies HAEndpointProvider. Returns every peer we've heard from
// with up/down inferred from last-seen vs LeaseTimeout. Ordinal is parsed
// from the node-id suffix (StatefulSet pod naming).
func (p *QuasarWriterProvider) Peers() []PeerResponse {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	out := make([]PeerResponse, 0, len(p.alive))
	for id, last := range p.alive {
		target := p.urls[id]
		if id == p.cfg.NodeID && target == "" {
			target = p.localTarget
		}
		out = append(out, PeerResponse{
			NodeID:   id,
			Target:   target,
			Up:       now.Sub(last) <= p.cfg.LeaseTimeout,
			Ordinal:  ordinalFromNodeID(id),
			LastSeen: last,
		})
	}
	sortPeersByOrdinal(out)
	return out
}

var (
	_ ha.LeaderProvider  = (*QuasarWriterProvider)(nil)
	_ HAEndpointProvider = (*QuasarWriterProvider)(nil)
)
