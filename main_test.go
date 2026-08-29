package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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

func TestBPlusTreeVerify(t *testing.T) {
	order := 128
	pageSize := 4096
	numKeys := 5000
	seed := int64(12345)

	dbPath := filepath.Join(t.TempDir(), "verify.db")
	tree := newTree(dbPath, pageSize, order)
	truth := make(map[string]string)

	rng := rand.New(rand.NewSource(seed))

	t.Logf("[1] Inserting %d random keys...", numKeys)
	insertedKeys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%06d_%d", rng.Intn(1000), i)
		val := fmt.Sprintf("val_%06d", i)
		tree.insert(key, val)
		if _, exists := truth[key]; !exists {
			insertedKeys = append(insertedKeys, key)
		}
		truth[key] = val
	}
	t.Logf("   Inserted %d unique keys.", len(insertedKeys))

	t.Log("[2] Verifying all inserted keys...")
	for _, key := range insertedKeys {
		treeVal, found := tree.search(key)
		if !found {
			t.Errorf("   MISSING: Key '%s' not found in B+Tree", key)
		} else if treeVal != truth[key] {
			t.Errorf("   MISMATCH: Key '%s' -> got '%s', expected '%s'", key, treeVal, truth[key])
		}
	}

	t.Log("[3] Checking non‑existent keys...")
	for i := 0; i < 100; i++ {
		fakeKey := fmt.Sprintf("fake_%d", rng.Intn(100000))
		if _, found := tree.search(fakeKey); found {
			t.Errorf("   PHANTOM: Key '%s' unexpectedly found", fakeKey)
		}
	}

	t.Log("[4] Testing range scans...")
	scanTestCases := []struct {
		name  string
		start string
		end   string
	}{
		{"First 100 keys", insertedKeys[0], insertedKeys[99]},
		{"Middle 200 keys", insertedKeys[400], insertedKeys[599]},
		{"Full dataset", insertedKeys[0], insertedKeys[len(insertedKeys)-1]},
		{"Empty range", "zzz_start", "zzz_end"},
		{"Inverted bounds", insertedKeys[500], insertedKeys[100]},
	}

	for _, tc := range scanTestCases {
		expected := make(map[string]string)
		for k, v := range truth {
			if k >= tc.start && k <= tc.end {
				expected[k] = v
			}
		}

		keys, vals := tree.scan(tc.start, tc.end)

		// Check length
		if len(keys) != len(expected) {
			t.Errorf("   SCAN %s: size mismatch: got %d, expected %d", tc.name, len(keys), len(expected))
			continue
		}

		for i := 0; i < len(keys); i++ {
			key := keys[i]
			val := vals[i]
			expVal, ok := expected[key]
			if !ok {
				t.Errorf("   SCAN %s: extra key '%s' returned", tc.name, key)
			} else if val != expVal {
				t.Errorf("   SCAN %s: value mismatch for '%s': got '%s', expected '%s'", tc.name, key, val, expVal)
			}
		}
	}

	t.Log("[5] Updating 20% of keys...")
	updateCount := len(insertedKeys) / 5
	for i := 0; i < updateCount; i++ {
		idx := rng.Intn(len(insertedKeys))
		key := insertedKeys[idx]
		newVal := fmt.Sprintf("updated_%d", i)
		tree.insert(key, newVal)
		truth[key] = newVal
	}

	t.Log("   Verifying updated keys...")
	for _, key := range insertedKeys {
		treeVal, found := tree.search(key)
		if !found {
			t.Errorf("   UPDATE CORRUPTION: key '%s' missing", key)
		} else if treeVal != truth[key] {
			t.Errorf("   UPDATE MISMATCH: key '%s' -> '%s', expected '%s'", key, treeVal, truth[key])
		}
	}

	t.Log("[6] Deleting 30% of keys...")
	deleteCount := len(insertedKeys) * 3 / 10
	rng.Shuffle(len(insertedKeys), func(i, j int) {
		insertedKeys[i], insertedKeys[j] = insertedKeys[j], insertedKeys[i]
	})
	toDelete := insertedKeys[:deleteCount]

	for _, key := range toDelete {
		tree.remove(key)
		delete(truth, key)
	}

	t.Log("   Verifying remaining keys...")
	for _, key := range insertedKeys[deleteCount:] {
		treeVal, found := tree.search(key)
		if !found {
			t.Errorf("   DELETE CORRUPTION: key '%s' was wrongly deleted", key)
		} else if treeVal != truth[key] {
			t.Errorf("   DELETE MISMATCH: key '%s' -> '%s', expected '%s'", key, treeVal, truth[key])
		}
	}

	t.Log("[7] Checking tree invariants...")
	if err := verifyTreeInvariants(tree); err != nil {
		t.Errorf("   STRUCTURAL ERROR: %v", err)
	}

	t.Log("[8] Re‑running scans after deletions...")
	for _, tc := range scanTestCases {
		expected := make(map[string]string)
		for k, v := range truth {
			if k >= tc.start && k <= tc.end {
				expected[k] = v
			}
		}
		keys, _ := tree.scan(tc.start, tc.end)
		if len(keys) != len(expected) {
			t.Errorf("   POST‑DELETE SCAN %s: size mismatch: got %d, expected %d", tc.name, len(keys), len(expected))
		}
	}

	t.Log("Verification PASSED – B+Tree matches map ground truth.")
}

func verifyTreeInvariants(tree *TreeRoot) error {
	if tree.root == 0 {
		return fmt.Errorf("root page is 0")
	}

	visited := make(map[int64]bool)
	err := verifyNode(tree, tree.root, visited, nil, nil)
	if err != nil {
		return err
	}

	return nil
}

func verifyNode(tree *TreeRoot, page int64, visited map[int64]bool, minKey, maxKey *string) error {
	if visited[page] {
		return fmt.Errorf("cycle detected at page %d", page)
	}
	visited[page] = true

	buf, err := tree.pager.readPage(page)
	if err != nil {
		return fmt.Errorf("failed to read page %d: %w", page, err)
	}
	node := decodeNode(buf, tree.order)

	for i := 0; i < len(node.keys)-1; i++ {
		if node.keys[i] >= node.keys[i+1] {
			return fmt.Errorf("keys not sorted at page %d: %s >= %s", page, node.keys[i], node.keys[i+1])
		}
	}

	if minKey != nil && len(node.keys) > 0 && node.keys[0] < *minKey {
		return fmt.Errorf("page %d has first key %s < minKey %s", page, node.keys[0], *minKey)
	}
	if maxKey != nil && len(node.keys) > 0 && node.keys[len(node.keys)-1] > *maxKey {
		return fmt.Errorf("page %d has last key %s > maxKey %s", page, node.keys[len(node.keys)-1], *maxKey)
	}

	if node.isLeaf {
		if len(node.children) != 0 {
			return fmt.Errorf("leaf page %d has children", page)
		}
	} else {
		if len(node.children) != len(node.keys)+1 {
			return fmt.Errorf("internal page %d has %d keys and %d children (should be %d)",
				page, len(node.keys), len(node.children), len(node.keys)+1)
		}
		for i, child := range node.children {
			var childMin, childMax *string
			if i < len(node.keys) {
				childMax = &node.keys[i]
			}
			if i > 0 {
				childMin = &node.keys[i-1]
			}
			if err := verifyNode(tree, child, visited, childMin, childMax); err != nil {
				return err
			}
		}
	}
	return nil
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
		temp := b.TempDir()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			path := filepath.Join(temp, fmt.Sprintf("bench_32_%d.db", i))
			tree := newTree(path, 4096, 32)
			for _, kv := range dataset {
				tree.insert(kv[0], kv[1])
			}
		}
	})

	b.Run("BPlusTree-Order64-Insert-10k", func(b *testing.B) {
		temp := b.TempDir()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			path := filepath.Join(temp, fmt.Sprintf("bench_32_%d.db", i))
			tree := newTree(path, 4096, 64)
			for _, kv := range dataset {
				tree.insert(kv[0], kv[1])
			}
		}
	})

	goMap := make(map[string]string)
	temp := b.TempDir()
	path := filepath.Join(temp, "bench_search.db")
	tree32 := newTree(path, 4096, 32)
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

func TestBPlusTreeSerializationRoundTrip(t *testing.T) {
	pageSize := 4096
	order := 32

	buf := make([]byte, pageSize)

	originalLeaf := &TreeNode{
		isLeaf:   true,
		order:    order,
		nextPage: 88,
		keys:     []string{"apple", "banana", "cherry"},
		values:   []string{"red", "yellow", "dark-red"},
	}

	leafBytes := encodeNode(originalLeaf, buf)
	recoveredLeaf := decodeNode(leafBytes, order)

	if len(recoveredLeaf.keys) != len(originalLeaf.keys) || recoveredLeaf.nextPage != 88 {
		t.Fatalf("Leaf header or size corrupted during byte translation.")
	}
	for i := 0; i < len(originalLeaf.keys); i++ {
		if recoveredLeaf.keys[i] != originalLeaf.keys[i] || recoveredLeaf.values[i] != originalLeaf.values[i] {
			t.Errorf("Leaf Data mismatch at index %d! Got %s:%s", i, recoveredLeaf.keys[i], recoveredLeaf.values[i])
		}
	}

	originalInternal := &TreeNode{
		isLeaf:   false,
		order:    order,
		keys:     []string{"grape", "lemon"},
		children: []int64{101, 102, 103},
	}

	internalBytes := encodeNode(originalInternal, buf)
	recoveredInternal := decodeNode(internalBytes, order)

	if len(recoveredInternal.children) != 3 || recoveredInternal.children[1] != 102 {
		t.Fatalf("Internal node child page links corrupt after decoding.")
	}
	t.Log("PASS: Pure-byte slotted-page serialization round-trip successful!")
}

func makeShuffledDataset(n int, seed int64) [][2]string {
	dataset := make([][2]string, n)
	for i := 0; i < n; i++ {
		dataset[i][0] = fmt.Sprintf("key_%05d", i)
		dataset[i][1] = fmt.Sprintf("value_%05d", i)
	}
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(dataset), func(i, j int) {
		dataset[i], dataset[j] = dataset[j], dataset[i]
	})
	return dataset
}
