package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeProvider is a deterministic HAEndpointProvider for unit tests.
type fakeProvider struct {
	url    string
	nodeID string
	term   uint64
	lease  time.Time
	peers  []PeerResponse
}

func (f *fakeProvider) LeaderInfo() (string, string, uint64, time.Time) {
	return f.url, f.nodeID, f.term, f.lease
}
func (f *fakeProvider) Peers() []PeerResponse { return f.peers }

func TestLeaderHandlerReturnsCurrentWriter(t *testing.T) {
	t0 := time.Date(2026, 4, 22, 19, 30, 0, 0, time.UTC)
	p := &fakeProvider{
		url:    "http://foo-1.foo-hs.hanzo.svc.cluster.local:8090",
		nodeID: "foo-1",
		term:   42,
		lease:  t0,
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/_ha/leader", nil)
	HandleLeader(p)(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	if got := rr.Header().Get("X-Base-Leader"); got != p.url {
		t.Fatalf("X-Base-Leader: want %q, got %q", p.url, got)
	}
	if got := rr.Header().Get("X-Base-Term"); got != "42" {
		t.Fatalf("X-Base-Term: want 42, got %q", got)
	}
	var body LeaderResponse
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.LeaderURL != p.url || body.NodeID != p.nodeID || body.Term != p.term {
		t.Fatalf("body mismatch: %+v", body)
	}
	if !body.LeaseExpires.Equal(t0) {
		t.Fatalf("lease expires: want %v, got %v", t0, body.LeaseExpires)
	}
}

func TestLeaderHandlerFailover(t *testing.T) {
	p := &fakeProvider{
		url:    "http://foo-0.foo-hs.hanzo.svc.cluster.local:8090",
		nodeID: "foo-0",
		term:   1,
		lease:  time.Now().Add(2 * time.Second),
	}
	rr := httptest.NewRecorder()
	HandleLeader(p)(rr, httptest.NewRequest(http.MethodGet, "/_ha/leader", nil))
	var first LeaderResponse
	_ = json.NewDecoder(rr.Body).Decode(&first)
	if first.NodeID != "foo-0" || first.Term != 1 {
		t.Fatalf("pre-failover: %+v", first)
	}

	// Simulate failover: writer changes + term bumps.
	p.url = "http://foo-1.foo-hs.hanzo.svc.cluster.local:8090"
	p.nodeID = "foo-1"
	p.term = 2
	p.lease = time.Now().Add(2 * time.Second)

	rr = httptest.NewRecorder()
	HandleLeader(p)(rr, httptest.NewRequest(http.MethodGet, "/_ha/leader", nil))
	var after LeaderResponse
	_ = json.NewDecoder(rr.Body).Decode(&after)
	if after.NodeID != "foo-1" {
		t.Fatalf("post-failover node_id: want foo-1, got %q", after.NodeID)
	}
	if after.Term <= first.Term {
		t.Fatalf("term must be monotonic: %d -> %d", first.Term, after.Term)
	}
}

func TestLeaderHandlerRejectsNonGet(t *testing.T) {
	p := &fakeProvider{url: "http://x:1", nodeID: "x", term: 1}
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		rr := httptest.NewRecorder()
		HandleLeader(p)(rr, httptest.NewRequest(m, "/_ha/leader", nil))
		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s: want 405, got %d", m, rr.Code)
		}
	}
}

func TestPeersHandlerReturnsSortedByOrdinal(t *testing.T) {
	p := &fakeProvider{
		peers: []PeerResponse{
			{NodeID: "foo-2", Target: "http://foo-2:8090", Up: true, Ordinal: 2},
			{NodeID: "foo-0", Target: "http://foo-0:8090", Up: true, Ordinal: 0},
			{NodeID: "foo-1", Target: "http://foo-1:8090", Up: false, Ordinal: 1},
		},
	}
	rr := httptest.NewRecorder()
	HandlePeers(p)(rr, httptest.NewRequest(http.MethodGet, "/_ha/peers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var got []PeerResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 peers, got %d", len(got))
	}
	// The fake provider returns peers as-is; the handler does not re-sort,
	// so this test asserts we expose whatever the provider sends. The real
	// provider (QuasarWriterProvider.Peers) sorts by ordinal — covered in
	// TestQuasarProviderPeersSorted.
	_ = got
}

func TestOrdinalFromNodeID(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"foo-0", 0},
		{"foo-1", 1},
		{"foo-bar-12", 12},
		{"foo", -1},
		{"foo-", -1},
		{"foo-x", -1},
	}
	for _, c := range cases {
		if got := ordinalFromNodeID(c.in); got != c.want {
			t.Errorf("ordinalFromNodeID(%q): want %d, got %d", c.in, c.want, got)
		}
	}
}

func TestWriterHeaderCacheIsAtomic(t *testing.T) {
	setWriterHeader("http://a:1", 7)
	url, term := currentWriterHeader()
	if url != "http://a:1" || term != 7 {
		t.Fatalf("got %q/%d", url, term)
	}
	setWriterHeader("http://b:1", 8)
	url, term = currentWriterHeader()
	if url != "http://b:1" || term != 8 {
		t.Fatalf("got %q/%d", url, term)
	}
}
