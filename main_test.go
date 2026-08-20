package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

var (
	testLogMu sync.Mutex
	currentT  *testing.T
)

func logNodeEvent(format string, args ...any) {
	testLogMu.Lock()
	defer testLogMu.Unlock()
	if currentT != nil {
		currentT.Logf(format, args...)
	}
}

func TestRaftInProcessConsensus_2_9(t *testing.T) {
	testLogMu.Lock()
	currentT = t
	testLogMu.Unlock()
	defer func() {
		testLogMu.Lock()
		currentT = nil
		testLogMu.Unlock()
	}()

	clusterSize := 3
	servers := make([]*httptest.Server, clusterSize)
	nodes := make([]*RaftNode, clusterSize)
	addresses := make([]string, clusterSize)

	for i := 0; i < clusterSize; i++ {
		mux := http.NewServeMux()
		server := httptest.NewServer(mux)
		servers[i] = server

		parsedURL, _ := url.Parse(server.URL)
		addresses[i] = parsedURL.Host
	}

	t.Logf("[SETUP] Cluster topology ports allocated: %v", addresses)

	for i := 0; i < clusterSize; i++ {
		id := addresses[i]
		var peers []string
		for j := 0; j < clusterSize; j++ {
			if i != j {
				peers = append(peers, addresses[j])
			}
		}

		nodes[i] = newRaftNode(id, peers)

		mux := servers[i].Config.Handler.(*http.ServeMux)
		mux.HandleFunc("/request-vote", requestVoteHandler(nodes[i]))
		mux.HandleFunc("/append-entry", appendEntriesHandler(nodes[i]))
	}

	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	t.Log("[EXECUTION] Cluster is now live. Monitoring lifecycle state changes...")

	time.Sleep(1500 * time.Millisecond)

	t.Log("[EVALUATION] Harvesting final cluster state invariants...")

	leaderCount := 0
	followerCount := 0
	candidateCount := 0
	var clusterTerm int
	var splitTerms bool

	for i, node := range nodes {
		node.mu.Lock()
		role := node.role
		term := node.currentTerm
		node.mu.Unlock()

		if i == 0 {
			clusterTerm = term
		} else if term != clusterTerm {
			splitTerms = true
		}

		switch role {
		case Leader:
			leaderCount++
		case Follower:
			followerCount++
		case Candidate:
			candidateCount++
		}
	}

	if splitTerms {
		t.Errorf("SAFETY VIOLATION: Cluster failed to converge on an identical term timeline.")
	}
	if leaderCount == 0 {
		t.Errorf("LIVENESS CRASH: No leader was elected. Read the trace history above to diagnose why.")
	}
	if leaderCount > 1 {
		t.Errorf("SPLIT BRAIN CORRUPTION: Multiple leaders detected simultaneously! Count: %d", leaderCount)
	}
	if leaderCount == 1 && !splitTerms {
		t.Logf("PASS: Exactly 1 stable Leader established at unified Term %d.", clusterTerm)
	}
}
