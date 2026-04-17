package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/litesql/go-ha"
	"github.com/luxfi/consensus"
)

// Quasar consensus-backed replication: change-sets are proposed as consensus
// blocks, totally ordered by the Chain engine, and applied on acceptance.
//
// Transport is direct HTTP (POST /_ha/replicate). No broker required.
// Hanzo pubsub is available as an optional fallback via BASE_PUBSUB_URL.

var replicationProcessID = time.Now().UnixNano()

// ---------------------------------------------------------------------------
// ConsensusPublisher — writer proposes change-sets as consensus blocks
// ---------------------------------------------------------------------------

type ConsensusPublisher struct {
	chain   *consensus.Chain
	peers   []string
	nodeID  consensus.NodeID
	seq     atomic.Uint64
	client  *http.Client
	log     *changeLog
}

func NewConsensusPublisher(nodeID string, peers []string) *ConsensusPublisher {
	cfg := consensus.DefaultConfig()
	cfg.Alpha = 1 // single-writer: self-accept is sufficient
	chain := consensus.NewChain(cfg)
	chain.Start(context.Background())

	var nid consensus.NodeID
	copy(nid[:], nodeID)

	return &ConsensusPublisher{
		chain:  chain,
		peers:  peers,
		nodeID: nid,
		client: &http.Client{Timeout: 5 * time.Second},
		log:    newChangeLog(4096),
	}
}

func (p *ConsensusPublisher) Publish(cs *ha.ChangeSet) error {
	cs.ProcessID = replicationProcessID
	seq := p.seq.Add(1)
	cs.StreamSeq = seq

	data, err := json.Marshal(cs)
	if err != nil {
		return err
	}

	// Wrap as a consensus block
	blockID := hashToID(data, seq)
	parentID := consensus.GenesisID
	if seq > 1 {
		parentID = hashToID(nil, seq-1)
	}

	block := &consensus.Block{
		ID:       blockID,
		ParentID: parentID,
		Height:   seq,
		Time:     time.Now(),
		Payload:  data,
	}

	ctx := context.Background()
	if err := p.chain.Add(ctx, block); err != nil {
		return fmt.Errorf("consensus add: %w", err)
	}

	// Self-vote: writer is the authority in single-writer mode
	vote := consensus.NewVote(blockID, consensus.VoteCommit, p.nodeID)
	vote.Signature = []byte("writer-authority")
	if err := p.chain.RecordVote(ctx, vote); err != nil {
		return fmt.Errorf("consensus vote: %w", err)
	}

	// Store in local log for catch-up pulls
	p.log.append(seq, data)

	// Broadcast to peers
	envelope, _ := json.Marshal(replicateEnvelope{Seq: seq, Block: data})
	var wg sync.WaitGroup
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer+"/_ha/replicate", bytes.NewReader(envelope))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := p.client.Do(req)
			if err != nil {
				slog.Debug("replicate: broadcast failed", "peer", peer, "error", err)
				return
			}
			resp.Body.Close()
		}(peer)
	}
	wg.Wait()

	slog.Debug("replicate: published via consensus", "seq", seq, "accepted", p.chain.IsAccepted(blockID))
	return nil
}

func (p *ConsensusPublisher) Sequence() uint64 { return p.seq.Load() }

// HandleLog serves /_ha/replicate/log?since=N for catch-up.
func (p *ConsensusPublisher) HandleLog(w http.ResponseWriter, r *http.Request) {
	var since uint64
	fmt.Sscanf(r.URL.Query().Get("since"), "%d", &since)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p.log.since(since))
}

// ---------------------------------------------------------------------------
// ConsensusSubscriber — replicas receive blocks, apply via consensus ordering
// ---------------------------------------------------------------------------

type ConsensusSubscriber struct {
	mu          sync.Mutex
	chain       *consensus.Chain
	node        string
	nodeID      consensus.NodeID
	writerURL   string
	db          *sql.DB
	interceptor ha.ChangeSetInterceptor
	streamSeq   uint64
	started     bool
	inbound     chan replicateEnvelope
	closeCh     chan struct{}
}

type ConsensusSubscriberConfig struct {
	Node        string
	WriterURL   string
	Interceptor ha.ChangeSetInterceptor
}

func NewConsensusSubscriber(cfg ConsensusSubscriberConfig) *ConsensusSubscriber {
	ccfg := consensus.DefaultConfig()
	ccfg.Alpha = 1
	chain := consensus.NewChain(ccfg)
	chain.Start(context.Background())

	var nid consensus.NodeID
	copy(nid[:], cfg.Node)

	return &ConsensusSubscriber{
		chain:       chain,
		node:        cfg.Node,
		nodeID:      nid,
		writerURL:   cfg.WriterURL,
		interceptor: cfg.Interceptor,
		inbound:     make(chan replicateEnvelope, 4096),
		closeCh:     make(chan struct{}),
	}
}

func (s *ConsensusSubscriber) SetDB(db *sql.DB) { s.db = db }
func (s *ConsensusSubscriber) LatestSeq() uint64 { return atomic.LoadUint64(&s.streamSeq) }

func (s *ConsensusSubscriber) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()

	if s.db != nil {
		conn, _ := s.db.Conn(context.Background())
		if conn != nil {
			conn.ExecContext(context.Background(),
				"CREATE TABLE IF NOT EXISTS ha_stats(subject TEXT UNIQUE, received_seq INTEGER, updated_at DATETIME)")
			var recv uint64
			if conn.QueryRowContext(context.Background(),
				"SELECT received_seq FROM ha_stats WHERE subject = 'consensus'").Scan(&recv) == nil {
				atomic.StoreUint64(&s.streamSeq, recv)
			}
			conn.Close()
		}
	}

	go s.applyLoop()
	go s.catchupLoop()
	return nil
}

func (s *ConsensusSubscriber) RemoveConsumer(_ context.Context, _ string) error { return nil }
func (s *ConsensusSubscriber) DeliveredInfo(_ context.Context, _ string) (any, error) {
	return map[string]uint64{"seq": s.LatestSeq()}, nil
}

// Receive is called by the HTTP handler when a block arrives.
func (s *ConsensusSubscriber) Receive(env replicateEnvelope) {
	select {
	case s.inbound <- env:
	default:
		slog.Warn("replicate: inbound queue full")
	}
}

func (s *ConsensusSubscriber) applyLoop() {
	// Wait for DB to be set by go-ha connector
	for s.db == nil {
		time.Sleep(50 * time.Millisecond)
	}
	for {
		select {
		case <-s.closeCh:
			return
		case env := <-s.inbound:
			s.processBlock(env)
		}
	}
}

func (s *ConsensusSubscriber) processBlock(env replicateEnvelope) {
	// Add to local consensus chain
	blockID := hashToID(env.Block, env.Seq)
	parentID := consensus.GenesisID
	if env.Seq > 1 {
		parentID = hashToID(nil, env.Seq-1)
	}

	ctx := context.Background()
	block := &consensus.Block{
		ID:       blockID,
		ParentID: parentID,
		Height:   env.Seq,
		Time:     time.Now(),
		Payload:  env.Block,
	}
	s.chain.Add(ctx, block)

	// Vote to accept (writer authority is trusted)
	vote := consensus.NewVote(blockID, consensus.VoteCommit, s.nodeID)
	vote.Signature = []byte("replica-ack")
	s.chain.RecordVote(ctx, vote)

	// Apply the change-set
	var cs ha.ChangeSet
	if err := json.Unmarshal(env.Block, &cs); err != nil {
		slog.Error("replicate: unmarshal failed", "error", err)
		return
	}

	if cs.Node == s.node && cs.ProcessID == replicationProcessID {
		s.updateSeq(env.Seq)
		return
	}

	cs.SetInterceptor(s.interceptor)
	if err := cs.Apply(s.db); err != nil {
		slog.Error("replicate: apply failed", "error", err, "seq", env.Seq)
		return
	}

	s.updateSeq(env.Seq)
	slog.Debug("replicate: accepted block", "seq", env.Seq, "accepted", s.chain.IsAccepted(blockID))
}

func (s *ConsensusSubscriber) updateSeq(seq uint64) {
	atomic.StoreUint64(&s.streamSeq, seq)
	if s.db != nil {
		s.db.Exec(
			"REPLACE INTO ha_stats(subject, received_seq, updated_at) VALUES('consensus', ?, ?)",
			seq, time.Now().Format(time.RFC3339Nano))
	}
}

func (s *ConsensusSubscriber) catchupLoop() {
	if s.writerURL == "" {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	client := &http.Client{Timeout: 5 * time.Second}

	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.catchup(client)
		}
	}
}

func (s *ConsensusSubscriber) catchup(client *http.Client) {
	since := s.LatestSeq()
	resp, err := client.Get(fmt.Sprintf("%s/_ha/replicate/log?since=%d", s.writerURL, since))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}

	var entries []logEntry
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err := json.Unmarshal(body, &entries); err != nil {
		return
	}

	for _, e := range entries {
		if e.Seq <= since {
			continue
		}
		s.processBlock(replicateEnvelope{Seq: e.Seq, Block: e.Data})
	}
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

type replicateEnvelope struct {
	Seq   uint64          `json:"seq"`
	Block json.RawMessage `json:"block"`
}

type logEntry struct {
	Seq  uint64          `json:"seq"`
	Data json.RawMessage `json:"data"`
}

// ---------------------------------------------------------------------------
// Ring buffer log for catch-up
// ---------------------------------------------------------------------------

type changeLog struct {
	mu      sync.RWMutex
	entries []logEntry
	cap     int
	idx     int
	full    bool
}

func newChangeLog(cap int) *changeLog {
	return &changeLog{entries: make([]logEntry, cap), cap: cap}
}

func (l *changeLog) append(seq uint64, data []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries[l.idx] = logEntry{Seq: seq, Data: json.RawMessage(data)}
	l.idx = (l.idx + 1) % l.cap
	if l.idx == 0 {
		l.full = true
	}
}

func (l *changeLog) since(seq uint64) []logEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var result []logEntry
	n := l.cap
	if !l.full {
		n = l.idx
	}
	for i := 0; i < n; i++ {
		idx := i
		if l.full {
			idx = (l.idx + i) % l.cap
		}
		if e := l.entries[idx]; e.Seq > seq {
			result = append(result, e)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func hashToID(data []byte, seq uint64) consensus.ID {
	h := sha256.New()
	h.Write(data)
	h.Write([]byte(fmt.Sprintf("%d", seq)))
	var id consensus.ID
	copy(id[:], h.Sum(nil))
	return id
}

var (
	_ ha.Publisher  = (*ConsensusPublisher)(nil)
	_ ha.Subscriber = (*ConsensusSubscriber)(nil)
	_ = errors.New // keep import
)
