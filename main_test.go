package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestRaftKVStore(t *testing.T) {
	clusterSize := 3
	servers := make([]*httptest.Server, clusterSize)
	nodes := make([]*RaftNode, clusterSize)
	addresses := make([]string, clusterSize)

	// Step 1: Initialize local HTTP mock servers
	for i := 0; i < clusterSize; i++ {
		mux := http.NewServeMux()
		server := httptest.NewServer(mux)
		servers[i] = server
		parsedURL, _ := url.Parse(server.URL)
		addresses[i] = parsedURL.Host
	}

	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	// Step 2: Boot up nodes and hook up all operational handlers
	for i := 0; i < clusterSize; i++ {
		id := addresses[i]
		var peers []string
		for j := 0; j < clusterSize; j++ {
			if i != j {
				peers = append(peers, addresses[j])
			}
		}

		nodes[i] = newRaftNode(id, peers)
		nodes[i].stateMachine = &KVstore{dict: make(map[string]string)}

		mux := servers[i].Config.Handler.(*http.ServeMux)
		mux.HandleFunc("/request-vote", requestVoteHandler(nodes[i]))
		mux.HandleFunc("/append-entry", appendEntriesHandler(nodes[i]))
		mux.HandleFunc("/put", putHandler(nodes[i]))
		mux.HandleFunc("/get", getHandler(nodes[i]))
		mux.HandleFunc("/remove", removeHandler(nodes[i]))
		mux.HandleFunc("/scan", scanHandler(nodes[i]))
	}

	time.Sleep(100 * time.Millisecond)

	// Step 3: Explicitly establish Node 0 as the current Leader
	leaderIdx := 0
	leaderNode := nodes[leaderIdx]
	leaderNode.mu.Lock()
	leaderNode.role = Leader
	leaderNode.currentTerm = 1
	leaderNode.id = addresses[leaderIdx]
	leaderNode.nextIndex = make(map[string]int)
	leaderNode.matchIndex = make(map[string]int)
	for _, peer := range leaderNode.peers {
		leaderNode.nextIndex[peer] = 1
		leaderNode.matchIndex[peer] = 0
	}
	leaderNode.mu.Unlock()

	// Direct followers to know who the leader is so HTTP redirection works
	for i, node := range nodes {
		if i != leaderIdx {
			node.mu.Lock()
			node.currentTerm = 1
			node.role = Follower
			node.leaderId = addresses[leaderIdx]
			node.mu.Unlock()
		}
	}

	// Kick off background heartbeats/replication on our designated leader
	go runHeartbeats(leaderNode, AppendEntriesArgs{})
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 300 * time.Millisecond}

	// ==========================================
	// ACTION 1: Test PUT across the cluster
	// ==========================================
	t.Log("[STAGE 1] Injecting sequential PUT operations via the leader...")
	kvPairs := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "dark-red",
	}

	for k, v := range kvPairs {
		putURL := "http://" + addresses[leaderIdx] + "/put?key=" + k + "&val=" + v
		resp, err := client.Post(putURL, "application/json", nil)
		if err != nil {
			t.Fatalf("PUT failed for %s: %v", k, err)
		}
		resp.Body.Close()
	}

	// Wait for consensus replication and background storage application
	time.Sleep(400 * time.Millisecond)

	// ==========================================
	// ACTION 2: Test GET on every node (Follower reads redirect or serve)
	// ==========================================
	t.Log("[STAGE 2] Verifying data replication with GET requests...")
	for idx, nodeAddr := range addresses {
		getURL := "http://" + nodeAddr + "/get?key=banana"
		resp, err := client.Get(getURL)
		if err != nil {
			t.Fatalf("GET failed on Node %d: %v", idx, err)
		}

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 OK on Node %d GET, got %d", idx, resp.StatusCode)
			resp.Body.Close()
			continue
		}

		var data struct {
			Value string `json:"value"`
		}
		json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()

		if data.Value != "yellow" {
			t.Errorf("Value mismatch on Node %d: got %s, expected 'yellow'", idx, data.Value)
		} else {
			t.Logf("PASS: GET 'banana' on Node %d returned '%s'", idx, data.Value)
		}
	}

	// ==========================================
	// ACTION 3: Test SCAN on the leader
	// ==========================================
	t.Log("[STAGE 3] Executing SCAN operation to check range queries...")
	scanURL := "http://" + addresses[leaderIdx] + "/scan?startKey=apple&endKey=banana"
	resp, err := client.Get(scanURL)
	if err != nil {
		t.Fatalf("SCAN request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected SCAN status 200 OK, got %d", resp.StatusCode)
	}

	var scanResult struct {
		Result map[string]string `json:"result"`
	}
	json.NewDecoder(resp.Body).Decode(&scanResult)
	resp.Body.Close()

	// Range [apple, banana] should capture "apple" and "banana", but exclude "cherry"
	if len(scanResult.Result) != 2 || scanResult.Result["apple"] != "red" || scanResult.Result["banana"] != "yellow" {
		t.Errorf("SCAN bounds evaluation failed. Got: %v", scanResult.Result)
	} else {
		t.Logf("PASS: SCAN [apple -> banana] returned correct subset: %v", scanResult.Result)
	}

	// ==========================================
	// ACTION 4: Test REMOVE replication
	// ==========================================
	t.Log("[STAGE 4] Testing REMOVE operation replication...")
	removeReq, _ := http.NewRequest(http.MethodDelete, "http://"+addresses[leaderIdx]+"/remove?key=apple", nil)
	removeResp, err := client.Do(removeReq)
	if err != nil {
		t.Fatalf("REMOVE request failed: %v", err)
	}
	removeResp.Body.Close()

	// Wait for the deletion log entry to traverse the cluster and execute
	time.Sleep(400 * time.Millisecond)

	// Validate 'apple' is missing across all storage engines
	for idx, node := range nodes {
		node.mu.Lock()
		_, found := node.stateMachine.dict["apple"]
		node.mu.Unlock()

		if found {
			t.Errorf("FAIL: Node %d still retains deleted key 'apple' in its state machine", idx)
		} else {
			t.Logf("PASS: Node %d successfully dropped 'apple'", idx)
		}
	}
}
