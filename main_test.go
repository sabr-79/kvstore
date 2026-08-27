package main

import (
	"fmt"
	"math/rand"
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

// Helper function to generate deterministic random string keys
func generateRandomKV(seed int64, count int) [][2]string {
	r := rand.New(rand.NewSource(seed))
	pairs := make([][2]string, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%d_%d", r.Intn(count*10), i)
		val := fmt.Sprintf("val_%d", r.Intn(100000))
		pairs[i] = [2]string{key, val}
	}
	return pairs
}

func TestBPlusTreeAgainstMap_Differential(t *testing.T) {
	order := 4
	tree := newTree(order)
	goMap := make(map[string]string)

	dataset := generateRandomKV(42, 10000)

	t.Logf("[STAGE 1] Feeding 10,000 sequential entries into B+Tree and Go Map...")
	for _, kv := range dataset {
		key, val := kv[0], kv[1]
		tree.insert(key, val)
		goMap[key] = val
	}

	t.Log("[STAGE 2] Verifying point lookups (Search) for every injected key...")
	for _, kv := range dataset {
		key, expectedVal := kv[0], kv[1]

		treeVal, found := tree.search(key)
		if !found {
			t.Fatalf("CORRUPTION FAILURE: Key '%s' exists in Ground Truth map but was not found in B+Tree.", key)
		}
		if treeVal != expectedVal {
			t.Fatalf("VALUE MISMATCH: For key '%s', B+Tree returned '%s'; expected '%s'", key, treeVal, expectedVal)
		}
	}

	t.Log("[STAGE 3] Querying non-existent random keys to verify clean miss handling...")
	for i := 0; i < 1000; i++ {
		fakeKey := fmt.Sprintf("non_existent_key_%d", i)
		_, found := tree.search(fakeKey)
		if found {
			t.Errorf("SAFETY BREACH: Search returned true for a non-existent phantom key: '%s'", fakeKey)
		}
	}

	t.Log("[STAGE 4] Executing range scans against map-filtered ground truth...")

	buildExpectedScan := func(start, end string) map[string]string {
		expected := make(map[string]string)
		for k, v := range goMap {
			if k >= start && k <= end {
				expected[k] = v
			}
		}
		return expected
	}

	scanTestCases := []struct {
		name  string
		start string
		end   string
	}{
		{"Standard Sub-Range", "key_1000_", "key_2000_"},
		{"Narrow Boundary", "key_500_0", "key_500_9"},
		{"Entire Dataset Sweep", "a", "z"}, // Strings catch all "key_" prefixes
		{"Completely Empty Range", "xyz_start", "xyz_end"},
		{"Inverted Bounds (Should be empty)", "key_9000_", "key_1000_"},
	}

	for _, tc := range scanTestCases {
		t.Logf("   Testing Range: [%s] -> [%s] (%s)", tc.start, tc.end, tc.name)

		expectedMap := buildExpectedScan(tc.start, tc.end)

		treeMap := tree.scan(tc.start, tc.end)

		if len(treeMap) != len(expectedMap) {
			t.Errorf("      FAIL (%s): Size mismatch! Tree returned %d keys, Expected %d keys.",
				tc.name, len(treeMap), len(expectedMap))
			continue
		}

		for expectedKey, expectedVal := range expectedMap {
			treeVal, exists := treeMap[expectedKey]
			if !exists {
				t.Errorf("      FAIL (%s): Key '%s' expected in scan result, but B+Tree missed it.",
					tc.name, expectedKey)
			}
			if treeVal != expectedVal {
				t.Errorf("      FAIL (%s): Key '%s' value mismatch. Tree: '%s', Expected: '%s'",
					tc.name, expectedKey, treeVal, expectedVal)
			}
		}
	}
	t.Log("[STAGE 5] Deleting a random subset of 3,000 keys and verifying state machine convergence...")

	r := rand.New(rand.NewSource(99))
	indicesToDelete := make(map[int]bool)
	for len(indicesToDelete) < 3000 {
		indicesToDelete[r.Intn(len(dataset))] = true
	}

	deletedCount := 0
	for idx, kv := range dataset {
		if indicesToDelete[idx] {
			key := kv[0]
			tree.remove(key)
			delete(goMap, key)
			deletedCount++
		}
	}
	t.Logf("   Successfully issued %d deletion requests.", deletedCount)

	t.Log("   Verifying integrity of remaining keys...")
	for _, kv := range dataset {
		key, val := kv[0], kv[1]

		treeVal, found := tree.search(key)
		_, expectedFound := goMap[key]

		if expectedFound {
			if !found {
				t.Errorf("POST-DELETE CORRUPTION: Key '%s' was accidentally lost during sibling node deletions.", key)
			}
			if treeVal != val {
				t.Errorf("POST-DELETE VALUE CORRUPTION: Key '%s' returned wrong value '%s' after structural slice shifts.", key, treeVal)
			}
		} else {
			if found {
				t.Errorf("LEAKED KEY RESIDUE: Key '%s' was deleted, but 'search' still returned true. Parallel values array likely out of alignment.", key)
			}
		}
	}

	t.Log("   Re-running range scans across deleted/fragmented nodes...")
	for _, tc := range scanTestCases {
		expectedMap := buildExpectedScan(tc.start, tc.end)
		treeMap := tree.scan(tc.start, tc.end)

		if len(treeMap) != len(expectedMap) {
			t.Errorf("      FAIL Post-Delete (%s): Range slice count discrepancy. Tree: %d, Expected: %d", tc.name, len(treeMap), len(expectedMap))
		}
	}

	t.Logf("SUCCESS: B+Tree with order %d perfectly matches plain map ground truth across 10,000 keys.", order)
}

func BenchmarkBPlusTreeVsMap(b *testing.B) {
	dataset := generateRandomKV(time.Now().UnixNano(), 10000)

	b.Run("GoMap-Insert-10k", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := make(map[string]string)
			for _, kv := range dataset {
				m[kv[0]] = kv[1]
			}
		}
	})

	b.Run("BPlusTree-Order32-Insert-10k", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tree := newTree(32)
			for _, kv := range dataset {
				tree.insert(kv[0], kv[1])
			}
		}
	})

	b.Run("BPlusTree-Order64-Insert-10k", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tree := newTree(64)
			for _, kv := range dataset {
				tree.insert(kv[0], kv[1])
			}
		}
	})

	goMap := make(map[string]string)
	tree32 := newTree(32)
	for _, kv := range dataset {
		goMap[kv[0]] = kv[1]
		tree32.insert(kv[0], kv[1])
	}

	b.Run("GoMap-Search-10k", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, kv := range dataset {
				_ = goMap[kv[0]]
			}
		}
	})

	b.Run("BPlusTree-Order32-Search-10k", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, kv := range dataset {
				_, _ = tree32.search(kv[0])
			}
		}
	})
}
