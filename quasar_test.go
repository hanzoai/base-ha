package main

import (
	"testing"
	"time"
)

// TestQuasarProviderFailoverBumpsTerm simulates foo-0 going silent and
// verifies (a) writer flips to foo-1, (b) term monotonically increments,
// (c) the atomic header cache reflects the new writer.
//
// Scenario runs from foo-1's point of view (not the dying writer's) so the
// self-heartbeat in heartbeatLoop doesn't keep resurrecting the failed node.
func TestQuasarProviderFailoverBumpsTerm(t *testing.T) {
	// Reset the global cache so tests are order-independent.
	writerHeaderCache.Store(nil)

	p, err := NewQuasarWriterProvider(QuasarWriterConfig{
		NodeID:            "foo-1",
		LocalTarget:       "http://foo-1:8090",
		HeartbeatInterval: 10 * time.Millisecond, // fast ticks for tests
		LeaseTimeout:      30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	defer p.Close()

	// Inject foo-0 as alive — lower-sorted, so foo-0 is the elected writer
	// and this node (foo-1) is the replica. Keep pumping heartbeats until
	// the provider elects it (races against the first heartbeat tick).
	waitFor(t, 200*time.Millisecond, func() bool {
		p.ingest(heartbeatMsg{NodeID: "foo-0", Target: "http://foo-0:8090"})
		_, id, _, _ := p.LeaderInfo()
		return id == "foo-0"
	})
	_, _, termBefore, _ := p.LeaderInfo()

	// Kill foo-0 by forcing its last-seen far into the past AND stop
	// re-ingesting it. Because foo-0 is NOT our NodeID, the periodic
	// self-refresh won't resurrect it.
	p.mu.Lock()
	p.alive["foo-0"] = time.Now().Add(-1 * time.Second)
	p.mu.Unlock()

	// Wait for re-election to foo-1.
	waitFor(t, 200*time.Millisecond, func() bool {
		_, id, _, _ := p.LeaderInfo()
		return id == "foo-1"
	})

	url, id, term, _ := p.LeaderInfo()
	if id != "foo-1" {
		t.Fatalf("post-failover writer: want foo-1, got %q", id)
	}
	if url != "http://foo-1:8090" {
		t.Fatalf("post-failover url: want http://foo-1:8090, got %q", url)
	}
	if term <= termBefore {
		t.Fatalf("term must increment on writer change: %d -> %d", termBefore, term)
	}

	// Atomic cache must reflect the new writer for the response-header middleware.
	cachedURL, cachedTerm := currentWriterHeader()
	if cachedURL != "http://foo-1:8090" {
		t.Fatalf("header cache url: %q", cachedURL)
	}
	if cachedTerm != term {
		t.Fatalf("header cache term: want %d, got %d", term, cachedTerm)
	}
}

// waitFor polls cond until it returns true or the deadline expires. Fails
// the test when the condition is never met. Used for deterministic waits
// in Quasar-provider failover tests.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition never true after %s", d)
	}
}

// TestQuasarProviderPeersSorted verifies the wire output is ordinal-sorted
// and reports up/down correctly based on lease timeout.
func TestQuasarProviderPeersSorted(t *testing.T) {
	writerHeaderCache.Store(nil)
	p, err := NewQuasarWriterProvider(QuasarWriterConfig{
		NodeID:            "foo-1",
		LocalTarget:       "http://foo-1:8090",
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseTimeout:      50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	defer p.Close()

	p.ingest(heartbeatMsg{NodeID: "foo-0", Target: "http://foo-0:8090"})
	p.ingest(heartbeatMsg{NodeID: "foo-2", Target: "http://foo-2:8090"})
	// Mark foo-2 as stale.
	p.mu.Lock()
	p.alive["foo-2"] = time.Now().Add(-1 * time.Second)
	p.mu.Unlock()

	peers := p.Peers()
	if len(peers) != 3 {
		t.Fatalf("want 3 peers, got %d: %+v", len(peers), peers)
	}
	if peers[0].NodeID != "foo-0" || peers[1].NodeID != "foo-1" || peers[2].NodeID != "foo-2" {
		t.Fatalf("not sorted by ordinal: %+v", peers)
	}
	if !peers[0].Up || !peers[1].Up {
		t.Fatalf("foo-0, foo-1 must be up: %+v", peers)
	}
	if peers[2].Up {
		t.Fatalf("foo-2 must be down (stale): %+v", peers[2])
	}
}
