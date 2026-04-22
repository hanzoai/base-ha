// Package main implements the /_ha/leader and /_ha/peers endpoints plus the
// X-Base-Leader response header. These are the source of truth for the
// hanzoai/gateway base_ha upstream.
//
// Contract (do not break):
//   GET /_ha/leader  -> {leader_url, node_id, term, lease_expires}
//   GET /_ha/peers   -> [{node_id, target, up, ordinal, last_seen}]
//   X-Base-Leader: <url>   on every response (best-effort cache for the gateway)
package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// LeaderResponse is the wire contract of GET /_ha/leader.
type LeaderResponse struct {
	LeaderURL    string    `json:"leader_url"`
	NodeID       string    `json:"node_id"`
	Term         uint64    `json:"term"`
	LeaseExpires time.Time `json:"lease_expires"`
}

// PeerResponse is the wire contract of a single peer in GET /_ha/peers.
type PeerResponse struct {
	NodeID   string    `json:"node_id"`
	Target   string    `json:"target"`
	Up       bool      `json:"up"`
	Ordinal  int       `json:"ordinal"`
	LastSeen time.Time `json:"last_seen"`
}

// HAEndpointProvider exposes the HA state the gateway needs. We keep a
// narrow interface so other leader providers can implement it later
// without changing the handlers.
type HAEndpointProvider interface {
	// LeaderInfo returns the current writer URL, the node-id of the writer,
	// the monotonic term (increments on writer change), and the lease
	// expiry timestamp (when the current writer's alive entry will go stale).
	LeaderInfo() (url, nodeID string, term uint64, leaseExpires time.Time)
	// Peers returns every peer we know about, with up/down inferred from
	// last-seen vs lease timeout.
	Peers() []PeerResponse
}

// HandleLeader serves GET /_ha/leader.
func HandleLeader(p HAEndpointProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		url, id, term, exp := p.LeaderInfo()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		// Also emit the tracking header so a client that misses the
		// poll can still learn the current writer from a single GET.
		w.Header().Set("X-Base-Leader", url)
		w.Header().Set("X-Base-Term", strconv.FormatUint(term, 10))
		_ = json.NewEncoder(w).Encode(LeaderResponse{
			LeaderURL:    url,
			NodeID:       id,
			Term:         term,
			LeaseExpires: exp,
		})
	}
}

// HandlePeers serves GET /_ha/peers.
func HandlePeers(p HAEndpointProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		peers := p.Peers()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(peers)
	}
}

// writerHeaderCache is an atomic snapshot of the current writer URL so
// the request-path middleware stamps a header with a single atomic load,
// never a lock.
var writerHeaderCache atomic.Pointer[writerHeader]

type writerHeader struct {
	url  string
	term uint64
}

// setWriterHeader is called from the heartbeat loop when the writer changes.
func setWriterHeader(url string, term uint64) {
	writerHeaderCache.Store(&writerHeader{url: url, term: term})
}

// currentWriterHeader returns the last snapshotted writer URL + term.
// Returns empty strings if we haven't elected a writer yet — callers must
// be tolerant of this (transient, at startup only).
func currentWriterHeader() (string, uint64) {
	wh := writerHeaderCache.Load()
	if wh == nil {
		return "", 0
	}
	return wh.url, wh.term
}

// ordinalFromNodeID returns the numeric suffix of a StatefulSet pod name
// like "foo-3" -> 3. Returns -1 if the node-id does not match the expected
// "<base>-<ordinal>" shape so the gateway can still rank by NodeID string.
func ordinalFromNodeID(id string) int {
	dash := strings.LastIndex(id, "-")
	if dash < 0 || dash == len(id)-1 {
		return -1
	}
	n, err := strconv.Atoi(id[dash+1:])
	if err != nil {
		return -1
	}
	return n
}

// sortPeersByOrdinal sorts peer responses by numeric ordinal (ascending)
// with NodeID as the tiebreaker so the gateway sees a stable order.
func sortPeersByOrdinal(peers []PeerResponse) {
	sort.Slice(peers, func(i, j int) bool {
		if peers[i].Ordinal != peers[j].Ordinal {
			return peers[i].Ordinal < peers[j].Ordinal
		}
		return peers[i].NodeID < peers[j].NodeID
	})
}
