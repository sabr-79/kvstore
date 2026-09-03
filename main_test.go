package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

func TestRaftKVStore(t *testing.T) {
	clusterSize := 3
	servers := make([]*httptest.Server, clusterSize)
	nodes := make([]*RaftNode, clusterSize)
	addresses := make([]string, clusterSize)

	tmpDirs := make([]string, clusterSize)
	for i := 0; i < clusterSize; i++ {
		tmpDirs[i] = t.TempDir()
	}

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

	for i := 0; i < clusterSize; i++ {
		id := addresses[i]
		var peers []string
		for j := 0; j < clusterSize; j++ {
			if i != j {
				peers = append(peers, addresses[j])
			}
		}

		nodes[i] = newRaftNode(id, peers)
		dbPath := filepath.Join(tmpDirs[i], "raft.db")
		tree := newTree(dbPath, 4096, 128)
		nodes[i].stateMachine = &KVstore{tree: tree}

		mux := servers[i].Config.Handler.(*http.ServeMux)
		mux.HandleFunc("/request-vote", requestVoteHandler(nodes[i]))
		mux.HandleFunc("/append-entry", appendEntriesHandler(nodes[i]))
		mux.HandleFunc("/put", putHandler(nodes[i]))
		mux.HandleFunc("/get", getHandler(nodes[i]))
		mux.HandleFunc("/remove", removeHandler(nodes[i]))
		mux.HandleFunc("/scan", scanHandler(nodes[i]))
	}

	time.Sleep(100 * time.Millisecond)

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

	for i, node := range nodes {
		if i != leaderIdx {
			node.mu.Lock()
			node.currentTerm = 1
			node.role = Follower
			node.leaderId = addresses[leaderIdx]
			node.mu.Unlock()
		}
	}

	go runHeartbeats(leaderNode, AppendEntriesArgs{})
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 300 * time.Millisecond}

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

	time.Sleep(400 * time.Millisecond)

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
		ResultK []string `json:"resultK"`
		ResultV []string `json:"resultV"`
	}
	json.NewDecoder(resp.Body).Decode(&scanResult)
	resp.Body.Close()

	if len(scanResult.ResultK) != 2 {
		t.Errorf("SCAN returned %d keys, expected 2", len(scanResult.ResultK))
	} else {
		t.Logf("SCAN ok: %v", scanResult.ResultK)
	}

	t.Log("[STAGE 4] Testing REMOVE operation replication...")
	removeReq, _ := http.NewRequest(http.MethodDelete, "http://"+addresses[leaderIdx]+"/remove?key=apple", nil)
	removeResp, err := client.Do(removeReq)
	if err != nil {
		t.Fatalf("REMOVE request failed: %v", err)
	}
	removeResp.Body.Close()

	time.Sleep(400 * time.Millisecond)

	for idx, node := range nodes {
		node.mu.Lock()
		_, found := node.stateMachine.tree.search("apple")
		node.mu.Unlock()

		if found {
			t.Errorf("FAIL: Node %d still retains deleted key 'apple' in its state machine", idx)
		} else {
			t.Logf("PASS: Node %d successfully dropped 'apple'", idx)
		}
	}
}
func TestRaftPersistenceCrashAndRecover(t *testing.T) {
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
		dbPath := filepath.Join(t.TempDir(), "test.db")
		nodes[i].stateMachine = &KVstore{tree: newTree(dbPath, 4096, 128)}

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
	val, found := nodes[1].stateMachine.tree.search("hello")
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
	numKeys := 1000000
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

func TestRaftThroughput(t *testing.T) {
	tmpDir := t.TempDir()
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
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	for i := 0; i < clusterSize; i++ {
		var peers []string
		for j := 0; j < clusterSize; j++ {
			if i != j {
				peers = append(peers, addresses[j])
			}
		}

		dbPath := filepath.Join(tmpDir, fmt.Sprintf("node_%d.db", i))
		node := newRaftNode(addresses[i], peers)
		node.batchsize = 100
		node.stateMachine = &KVstore{tree: newTree(dbPath, 4096, 128)}
		node.stateMachine.tree.batchsize = 1000
		nodes[i] = node

		mux := servers[i].Config.Handler.(*http.ServeMux)
		mux.HandleFunc("/request-vote", requestVoteHandler(node))
		mux.HandleFunc("/append-entry", appendEntriesHandler(node))
		mux.HandleFunc("/put", putHandler(node))
		mux.HandleFunc("/get", getHandler(node))
	}

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

	for i, node := range nodes {
		if i != leaderIdx {
			node.mu.Lock()
			node.currentTerm = 1
			node.role = Follower
			node.leaderId = addresses[leaderIdx]
			node.mu.Unlock()
		}
	}

	go runHeartbeats(leaderNode, AppendEntriesArgs{})
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 20 * time.Second}

	const N = 10000

	putLatencies := make([]time.Duration, N)
	putStart := time.Now()
	putErrs := 0
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		url := fmt.Sprintf("http://%s/put?key=%s&val=%s", addresses[leaderIdx], key, val)
		opStart := time.Now()
		resp, err := client.Post(url, "application/json", nil)
		putLatencies[i] = time.Since(opStart)
		if err != nil {
			t.Logf("PUT error: %v", err)
			putErrs++
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logf("PUT status: %d", resp.StatusCode)
			putErrs++
		}
	}
	putDur := time.Since(putStart)
	p50p, p95p, p99p := calcPercentiles(putLatencies)
	printResult("Raft PUT", N, putDur, p50p, p95p, p99p, putErrs)

	getLatencies := make([]time.Duration, N)
	getStart := time.Now()
	getErrs := 0
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key_%d", i)
		url := fmt.Sprintf("http://%s/get?key=%s", addresses[leaderIdx], key)
		opStart := time.Now()
		resp, err := client.Get(url)
		getLatencies[i] = time.Since(opStart)
		if err != nil {
			t.Logf("GET error: %v", err)
			getErrs++
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logf("GET status: %d", resp.StatusCode)
			getErrs++
		}
	}
	getDur := time.Since(getStart)
	p50g, p95g, p99g := calcPercentiles(getLatencies)
	printResult("Raft GET", N, getDur, p50g, p95g, p99g, getErrs)
}
func TestBPlusTreeManyOrders(t *testing.T) {
	const N = 10000
	orders := [][]string{
		func() []string {
			keys := make([]string, N)
			for i := 0; i < N; i++ {
				keys[i] = fmt.Sprintf("key_%d", i)
			}
			return keys
		}(),
		func() []string {
			keys := make([]string, N)
			for i := 0; i < N; i++ {
				keys[i] = fmt.Sprintf("key_%d", N-1-i)
			}
			return keys
		}(),
	}
	for seed := 0; seed < 5; seed++ {
		r := rand.New(rand.NewSource(int64(seed)))
		keys := make([]string, N)
		for i := 0; i < N; i++ {
			keys[i] = fmt.Sprintf("key_%d", i)
		}
		r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		orders = append(orders, keys)
	}

	for orderIdx, keys := range orders {
		tree := newTree(filepath.Join(t.TempDir(), fmt.Sprintf("order_%d.db", orderIdx)), 4096, 128)
		for _, key := range keys {
			tree.insert(key, "val_"+key)
		}
		for _, key := range keys {
			if _, found := tree.search(key); !found {
				t.Fatalf("Order %d: key %s missing", orderIdx, key)
			}
		}
		if err := verifyTreeInvariants(tree); err != nil {
			t.Fatalf("Order %d: invariant violation: %v", orderIdx, err)
		}
	}
}

func TestRaftThroughput2(t *testing.T) {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		MaxConnsPerHost:     50,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}
	defer transport.CloseIdleConnections()

	tmpDir := t.TempDir()
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
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	for i := 0; i < clusterSize; i++ {
		var peers []string
		for j := 0; j < clusterSize; j++ {
			if i != j {
				peers = append(peers, addresses[j])
			}
		}

		dbPath := filepath.Join(tmpDir, fmt.Sprintf("node_%d.db", i))
		node := newRaftNode(addresses[i], peers)
		node.batchsize = 1000
		node.snapsize = 0
		node.stateMachine = &KVstore{tree: newTree(dbPath, 4096, 128)}
		node.stateMachine.tree.batchsize = 1000
		nodes[i] = node

		mux := servers[i].Config.Handler.(*http.ServeMux)
		mux.HandleFunc("/request-vote", requestVoteHandler(node))
		mux.HandleFunc("/append-entry", appendEntriesHandler(node))
		mux.HandleFunc("/put", putHandler(node))
		mux.HandleFunc("/get", getHandler(node))
	}

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

	for i, node := range nodes {
		if i != leaderIdx {
			node.mu.Lock()
			node.currentTerm = 1
			node.role = Follower
			node.leaderId = addresses[leaderIdx]
			node.mu.Unlock()
		}
	}

	go runHeartbeats(leaderNode, AppendEntriesArgs{})
	time.Sleep(100 * time.Millisecond)

	const N = 20000
	const maxConcurrent = 20

	putLatencies := make([]time.Duration, N)
	putStart := time.Now()
	putErrs := 0
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrent)

	for i := 0; i < N; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			key := fmt.Sprintf("key_%d", idx)
			val := fmt.Sprintf("val_%d", idx)
			url := fmt.Sprintf("http://%s/put?key=%s&val=%s", addresses[leaderIdx], key, val)
			opStart := time.Now()
			resp, err := client.Post(url, "application/json", nil)
			putLatencies[idx] = time.Since(opStart)
			if err != nil {
				t.Logf("PUT error: %v", err)
				putErrs++
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Logf("PUT status: %d", resp.StatusCode)
				putErrs++
			}
		}(i)
	}
	wg.Wait()
	putDur := time.Since(putStart)
	p50p, p95p, p99p := calcPercentiles(putLatencies)
	printResult("Raft PUT", N, putDur, p50p, p95p, p99p, putErrs)

	time.Sleep(500 * time.Millisecond)

	leaderNode.mu.Lock()
	var insertionOrder []string
	for _, entry := range leaderNode.log {
		if strings.HasPrefix(entry.Command, "PUT:") {
			parts := strings.Split(entry.Command, ":")
			insertionOrder = append(insertionOrder, parts[1])
		}
	}
	leaderNode.mu.Unlock()

	debugDBPath := filepath.Join(tmpDir, "debug_replay.db")
	debugTree := newTree(debugDBPath, 4096, 128)
	for _, key := range insertionOrder {
		debugTree.insert(key, "val_"+key)
	}

	missing := 0
	for _, key := range insertionOrder {
		if _, found := debugTree.search(key); !found {
			missing++
			t.Logf("Missing in debug tree: %s", key)
		}
	}
	t.Logf("Debug replay: %d keys, %d missing", len(insertionOrder), missing)
	if missing > 0 {
		t.Fatalf("B+ tree corrupted for this insertion order: %d keys lost", missing)
	}

	t.Log("Waiting for state machine to apply all entries...")

	var currentLeader *RaftNode
	for {
		for _, node := range nodes {
			node.mu.Lock()
			if node.role == Leader {
				currentLeader = node
				node.mu.Unlock()
				break
			}
			node.mu.Unlock()
		}
		if currentLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("Current leader: %s", currentLeader.id)

	for {
		currentLeader.mu.Lock()
		applied := currentLeader.lastApplied
		commitIdx := currentLeader.commitIndex
		currentLeader.mu.Unlock()
		if applied >= commitIdx {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Log("All entries applied on current leader.")

	getLatencies := make([]time.Duration, N)
	getStart := time.Now()
	getErrs := 0
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key_%d", i)
		url := fmt.Sprintf("http://%s/get?key=%s", currentLeader.id, key)

		opStart := time.Now()
		var resp *http.Response
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			resp, err = client.Get(url)
			if err == nil {
				break
			}
			if attempt < 2 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		getLatencies[i] = time.Since(opStart)
		if err != nil {
			t.Logf("GET error: %v", err)
			getErrs++
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logf("GET status: %d", resp.StatusCode)
			getErrs++
		}
	}
	getDur := time.Since(getStart)
	p50g, p95g, p99g := calcPercentiles(getLatencies)
	printResult("Raft GET", N, getDur, p50g, p95g, p99g, getErrs)
}

func TestRaftSnapshotCatchUp(t *testing.T) {
	tmpDir := t.TempDir()
	clusterSize := 3
	servers := make([]*httptest.Server, clusterSize)
	nodes := make([]*RaftNode, clusterSize)
	addresses := make([]string, clusterSize)

	// Start HTTP servers
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

	for i := 0; i < clusterSize; i++ {
		id := addresses[i]
		var peers []string
		for j := 0; j < clusterSize; j++ {
			if i != j {
				peers = append(peers, addresses[j])
			}
		}

		dbPath := filepath.Join(tmpDir, fmt.Sprintf("node_%d.db", i))
		node := newRaftNode(id, peers)
		node.stateMachine = &KVstore{tree: newTree(dbPath, 4096, 128)}
		nodes[i] = node

		mux := servers[i].Config.Handler.(*http.ServeMux)
		mux.HandleFunc("/request-vote", requestVoteHandler(node))
		mux.HandleFunc("/append-entry", appendEntriesHandler(node))
		mux.HandleFunc("/install-snapshot", installSnapshotHandler(node))
		mux.HandleFunc("/put", putHandler(node))
		mux.HandleFunc("/get", getHandler(node))
	}

	leader := nodes[0]
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.nextIndex = make(map[string]int)
	leader.matchIndex = make(map[string]int)
	for _, peer := range leader.peers {
		leader.nextIndex[peer] = 1
		leader.matchIndex[peer] = 0
	}
	leader.mu.Unlock()

	for i := 1; i < clusterSize; i++ {
		nodes[i].mu.Lock()
		nodes[i].currentTerm = 1
		nodes[i].role = Follower
		nodes[i].leaderId = addresses[0]
		nodes[i].mu.Unlock()
	}

	go runHeartbeats(leader, AppendEntriesArgs{})
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}
	putKey := func(key, val string) {
		url := fmt.Sprintf("http://%s/put?key=%s&val=%s", addresses[0], key, val)
		resp, err := client.Post(url, "application/json", nil)
		if err != nil {
			t.Fatalf("PUT %s failed: %v", key, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s status %d", key, resp.StatusCode)
		}
	}

	for i := 0; i < 200; i++ {
		putKey(fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i))
	}
	if err := leader.takeSnapshot(); err != nil {
		t.Fatalf("takeSnapshot: %v", err)
	}

	leader.mu.Lock()
	snapIdx := leader.snapIndex
	leader.mu.Unlock()
	if snapIdx == 0 {
		t.Fatalf("Expected snapshot index > 0, got %d", snapIdx)
	}
	t.Logf("Leader snapshot index: %d", snapIdx)

	target := nodes[2]
	target.mu.Lock()
	target.isDead = true
	target.mu.Unlock()

	for i := 200; i < 700; i++ {
		putKey(fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i))
	}

	leader.mu.Lock()
	leader.nextIndex[target.id] = 1
	leader.mu.Unlock()

	target.mu.Lock()
	target.isDead = false
	target.mu.Unlock()

	leader.mu.Lock()
	leaderCommit := leader.commitIndex
	leader.mu.Unlock()
	t.Logf("Leader commit index: %d", leaderCommit)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		target.mu.Lock()
		applied := target.lastApplied
		commit := target.commitIndex
		target.mu.Unlock()
		if applied >= leaderCommit && commit >= leaderCommit {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	target.mu.Lock()
	tree := target.stateMachine.tree
	target.mu.Unlock()
	for i := 0; i < 700; i++ {
		key := fmt.Sprintf("key_%d", i)
		if _, found := tree.search(key); !found {
			t.Errorf("Follower missing key %s", key)
		}
	}
}
