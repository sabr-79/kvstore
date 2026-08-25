package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

func TestRaftPersistenceCrashAndRecover_Phase4(t *testing.T) {
	clusterSize := 3
	servers := make([]*httptest.Server, clusterSize)
	nodes := make([]*RaftNode, clusterSize)
	addresses := make([]string, clusterSize)

	for i := 0; i < clusterSize; i++ {
		os.Remove("raft_node_" + string(rune('0'+i)) + ".state")
	}
	defer func() {
		for i := 0; i < clusterSize; i++ {
			os.Remove("raft_node_" + string(rune('0'+i)) + ".state")
			if servers[i] != nil {
				servers[i].Close()
			}
		}
	}()

	for i := 0; i < clusterSize; i++ {
		mux := http.NewServeMux()
		server := httptest.NewServer(mux)
		servers[i] = server
		parsedURL, _ := url.Parse(server.URL)
		addresses[i] = parsedURL.Host
	}

	for i := 0; i < clusterSize; i++ {
		var peers []string
		for j := 0; j < clusterSize; j++ {
			if i != j {
				peers = append(peers, addresses[j])
			}
		}

		mockID := fmt.Sprintf("node_%d", i)
		nodes[i] = newRaftNode(mockID, peers)
		nodes[i].id = mockID
		nodes[i].stateMachine = &KVstore{dict: make(map[string]string)}

		mux := servers[i].Config.Handler.(*http.ServeMux)
		mux.HandleFunc("/append-entry", appendEntriesHandler(nodes[i]))
		mux.HandleFunc("/put", putHandler(nodes[i]))
		mux.HandleFunc("/get", getHandler(nodes[i]))
	}

	nodes[0].mu.Lock()
	nodes[0].role = Leader
	nodes[0].currentTerm = 1
	nodes[0].nextIndex = make(map[string]int)
	nodes[0].matchIndex = make(map[string]int)
	for _, peer := range nodes[0].peers {
		nodes[0].nextIndex[peer] = 1
		nodes[0].matchIndex[peer] = 0
	}
	nodes[0].mu.Unlock()

	for i := 1; i < clusterSize; i++ {
		nodes[i].mu.Lock()
		nodes[i].currentTerm = 1
		nodes[i].role = Follower
		nodes[i].leaderId = addresses[0]
		nodes[i].mu.Unlock()
	}

	go runHeartbeats(nodes[0], AppendEntriesArgs{})
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 200 * time.Millisecond}

	t.Log("[CRASH TEST] Injecting initial key...")
	resp, _ := client.Post("http://"+addresses[0]+"/put?key=foo&val=bar", "application/json", nil)
	resp.Body.Close()
	time.Sleep(100 * time.Millisecond)

	t.Log("[CRASH TEST] Sudden crash of Node 1! Cutting off its network handlers...")
	nodes[1].mu.Lock()
	nodes[1].isDead = true
	nodes[1].mu.Unlock()

	t.Log("[CRASH TEST] Sending updates to remaining healthy quorum...")
	resp2, err := client.Post("http://"+addresses[0]+"/put?key=hello&val=world", "application/json", nil)
	if err != nil {
		t.Fatalf("Quorum write failed while 1 node was down: %v", err)
	}
	resp2.Body.Close()
	time.Sleep(100 * time.Millisecond)

	t.Log("[CRASH TEST] Simulating RAM wipe on Node 1. Restoring state fields exclusively from disk...")
	nodes[1].mu.Lock()
	nodes[1].log = nil
	nodes[1].currentTerm = 0
	nodes[1].votedFor = ""
	nodes[1].mu.Unlock()

	t.Log("[CRASH TEST] Re-booting Node 1. Invoking readPersistFile...")
	readPersistFile(nodes[1])

	nodes[1].mu.Lock()
	nodes[1].isDead = false
	nodes[1].mu.Unlock()

	t.Log("[CRASH TEST] Node 1 is back online. Waiting for catch-up replication...")
	time.Sleep(250 * time.Millisecond)

	t.Log("[CRASH TEST] Validating state machine catch-up on recovered node...")
	nodes[1].mu.Lock()
	val, found := nodes[1].stateMachine.dict["hello"]
	logLen := len(nodes[1].log)
	nodes[1].mu.Unlock()

	if !found {
		t.Errorf("FAIL: Recovered node missed data updates broadcasted during its crash down-time.")
	} else if val != "world" {
		t.Errorf("FAIL: Recovered node data corrupt. Got '%s', expected 'world'", val)
	} else {
		t.Logf("SUCCESS: Node 1 successfully survived crash-recovery! Restored Log Length: %d, Key 'hello' = '%s'", logLen, val)
	}
}
