package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	_ "modernc.org/sqlite"
)

const (
	batchSize = 1000
	pageSize  = 4096
	order     = 128
	scanRange = 100

	minCacheBytes = 64 * 1024
	cacheFraction = 0.5
)

func calcPercentiles(lat []time.Duration) (p50, p95, p99 time.Duration) {
	if len(lat) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(lat))
	copy(sorted, lat)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 = sorted[int(float64(len(sorted))*0.50)]
	p95 = sorted[int(float64(len(sorted))*0.95)]
	p99 = sorted[int(float64(len(sorted))*0.99)]
	return
}

func printResult(name string, ops int, dur time.Duration, p50, p95, p99 time.Duration, errs int) {
	qps := float64(ops) / dur.Seconds()
	fmt.Printf("%-22s | %7d ops | %9.0f qps | p50=%7v | p95=%7v | p99=%7v | errs=%d\n",
		name, ops, qps, p50, p95, p99, errs)
}

func cacheBudgetBytes(actualBytesUsed int64) int64 {
	budget := int64(float64(actualBytesUsed) * cacheFraction)
	if budget < minCacheBytes {
		budget = minCacheBytes
	}
	return budget
}

func safeIntn(rng *rand.Rand, n int) int {
	if n <= 0 {
		return 0
	}
	return rng.Intn(n)
}

func getRangeKeys(sortedDataset [][2]string, n, scanRange int, rng *rand.Rand) (startKey, endKey string) {
	if n <= scanRange {
		return sortedDataset[0][0], sortedDataset[n-1][0]
	}
	startIdx := safeIntn(rng, n-scanRange)
	return sortedDataset[startIdx][0], sortedDataset[startIdx+scanRange][0]
}

func TestFullBenchmark(t *testing.T) {
	datasetSizes := []int{100, 1000, 10000, 100000, 1000000}

	for _, n := range datasetSizes {
		t.Run(fmt.Sprintf("dataset_%d", n), func(t *testing.T) {
			fmt.Printf("\n=== Dataset: %d keys ===\n", n)

			dataset := makeShuffledDataset(n, 12345)
			sortedDataset := make([][2]string, n)
			copy(sortedDataset, dataset)
			sort.Slice(sortedDataset, func(i, j int) bool {
				return sortedDataset[i][0] < sortedDataset[j][0]
			})

			runBPlusTreeFull(t, n, dataset, sortedDataset)
			runSQLiteFull(t, n, dataset, sortedDataset)
			runPebbleFull(t, n, dataset, sortedDataset)
		})
	}
}

// B+TREE
func runBPlusTreeFull(t *testing.T, n int, dataset, sortedDataset [][2]string) {
	dbPath := filepath.Join(t.TempDir(), "btree.db")
	tree := newTree(dbPath, pageSize, order)
	tree.batchsize = batchSize

	startWrite := time.Now()
	for i := 0; i < n; i++ {
		tree.insert(dataset[i][0], dataset[i][1])
	}
	tree.pager.flush()
	writeDur := time.Since(startWrite)
	qpsWrite := float64(n) / writeDur.Seconds()

	actualBytes := tree.maxpage * int64(pageSize)
	cacheBytes := cacheBudgetBytes(actualBytes)
	cachePages := int(cacheBytes / int64(pageSize))
	if cachePages < 1 {
		cachePages = 1
	}
	tree.pager.maxClean = cachePages
	fmt.Printf("  B+Tree cache: %d pages (%d KB) | actual data: %d KB\n",
		cachePages, cacheBytes>>10, actualBytes>>10)

	// Warm random reads
	latWarm := make([]time.Duration, n)
	startWarm := time.Now()
	errWarm := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, found := tree.search(dataset[i][0])
		latWarm[i] = time.Since(opStart)
		if !found {
			errWarm++
		}
	}
	readDurWarm := time.Since(startWarm)
	p50w, p95w, p99w := calcPercentiles(latWarm)
	printResult("B+Tree warm reads", n, readDurWarm, p50w, p95w, p99w, errWarm)

	// Sequential reads
	latSeq := make([]time.Duration, n)
	startSeq := time.Now()
	errSeq := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, found := tree.search(sortedDataset[i][0])
		latSeq[i] = time.Since(opStart)
		if !found {
			errSeq++
		}
	}
	readDurSeq := time.Since(startSeq)
	p50seq, p95seq, p99seq := calcPercentiles(latSeq)
	printResult("B+Tree seq reads", n, readDurSeq, p50seq, p95seq, p99seq, errSeq)

	// Cold random reads
	tree.pager.close()
	tree = newTree(dbPath, pageSize, order)
	tree.pager.maxClean = cachePages

	latCold := make([]time.Duration, n)
	startCold := time.Now()
	errCold := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, found := tree.search(dataset[i][0])
		latCold[i] = time.Since(opStart)
		if !found {
			errCold++
		}
	}
	readDurCold := time.Since(startCold)
	p50c, p95c, p99c := calcPercentiles(latCold)
	printResult("B+Tree cold reads", n, readDurCold, p50c, p95c, p99c, errCold)

	// Range scans
	if n > scanRange {
		scanCount := min(n, 100)
		latScan := make([]time.Duration, scanCount)
		startScan := time.Now()
		errScan := 0
		rng := rand.New(rand.NewSource(12345))
		for i := 0; i < scanCount; i++ {
			startKey, endKey := getRangeKeys(sortedDataset, n, scanRange, rng)
			opStart := time.Now()
			_, values := tree.scan(startKey, endKey)
			latScan[i] = time.Since(opStart)
			if len(values) < scanRange/2 {
				errScan++
			}
		}
		scanDur := time.Since(startScan)
		p50s, p95s, p99s := calcPercentiles(latScan)
		printResult("B+Tree range scans", scanCount, scanDur, p50s, p95s, p99s, errScan)
	}

	fmt.Printf("  (write QPS: %.0f)\n", qpsWrite)
}

// SQLITE
func runSQLiteFull(t *testing.T, n int, dataset, sortedDataset [][2]string) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")

	db, err := sql.Open("sqlite", dbPath+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("Failed to open SQLite: %v", err)
	}
	db.Exec("PRAGMA page_size=4096")
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=FULL")
	db.Exec("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")

	startWrite := time.Now()
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT OR REPLACE INTO kv VALUES(?, ?)")
	for i := 0; i < n; i++ {
		stmt.Exec(dataset[i][0], dataset[i][1])
		if i%batchSize == 0 && i > 0 {
			tx.Commit()
			tx, _ = db.Begin()
			stmt, _ = tx.Prepare("INSERT OR REPLACE INTO kv VALUES(?, ?)")
		}
	}
	tx.Commit()
	writeDur := time.Since(startWrite)
	qpsWrite := float64(n) / writeDur.Seconds()

	var pageCount int
	db.QueryRow("PRAGMA page_count").Scan(&pageCount)
	actualBytes := int64(pageCount) * 4096
	cacheBytes := cacheBudgetBytes(actualBytes)
	cachePages := int(cacheBytes / 4096)
	if cachePages < 1 {
		cachePages = 1
	}
	fmt.Printf("  SQLite cache: %d pages (%d KB) | actual data: %d KB\n",
		cachePages, cacheBytes>>10, actualBytes>>10)
	db.Close()

	openWithCache := func() *sql.DB {
		d, _ := sql.Open("sqlite", dbPath+"?cache=shared&mode=ro")
		d.Exec(fmt.Sprintf("PRAGMA cache_size=%d", cachePages))
		return d
	}

	// Cold reads
	db = openWithCache()
	latCold := make([]time.Duration, n)
	startCold := time.Now()
	errCold := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		var val string
		err := db.QueryRow("SELECT value FROM kv WHERE key = ?", dataset[i][0]).Scan(&val)
		latCold[i] = time.Since(opStart)
		if err != nil {
			errCold++
		}
	}
	readDurCold := time.Since(startCold)
	p50c, p95c, p99c := calcPercentiles(latCold)
	printResult("SQLite cold reads", n, readDurCold, p50c, p95c, p99c, errCold)
	db.Close()

	// Warm reads
	db = openWithCache()
	latWarm := make([]time.Duration, n)
	startWarm := time.Now()
	errWarm := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		var val string
		err := db.QueryRow("SELECT value FROM kv WHERE key = ?", dataset[i][0]).Scan(&val)
		latWarm[i] = time.Since(opStart)
		if err != nil {
			errWarm++
		}
	}
	readDurWarm := time.Since(startWarm)
	p50w, p95w, p99w := calcPercentiles(latWarm)
	printResult("SQLite warm reads", n, readDurWarm, p50w, p95w, p99w, errWarm)
	db.Close()

	// Sequential reads
	db = openWithCache()
	defer db.Close()
	latSeq := make([]time.Duration, n)
	startSeq := time.Now()
	errSeq := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		var val string
		err := db.QueryRow("SELECT value FROM kv WHERE key = ?", sortedDataset[i][0]).Scan(&val)
		latSeq[i] = time.Since(opStart)
		if err != nil {
			errSeq++
		}
	}
	readDurSeq := time.Since(startSeq)
	p50seq, p95seq, p99seq := calcPercentiles(latSeq)
	printResult("SQLite seq reads", n, readDurSeq, p50seq, p95seq, p99seq, errSeq)

	// Range scans
	if n > scanRange {
		scanCount := min(n, 100)
		latScan := make([]time.Duration, scanCount)
		startScan := time.Now()
		errScan := 0
		rng := rand.New(rand.NewSource(12345))
		for i := 0; i < scanCount; i++ {
			startKey, endKey := getRangeKeys(sortedDataset, n, scanRange, rng)
			opStart := time.Now()
			rows, err := db.Query("SELECT value FROM kv WHERE key >= ? AND key < ?", startKey, endKey)
			if err != nil {
				errScan++
				continue
			}
			count := 0
			for rows.Next() {
				var val string
				rows.Scan(&val)
				count++
			}
			rows.Close()
			latScan[i] = time.Since(opStart)
			if count < scanRange/2 {
				errScan++
			}
		}
		scanDur := time.Since(startScan)
		p50s, p95s, p99s := calcPercentiles(latScan)
		printResult("SQLite range scans", scanCount, scanDur, p50s, p95s, p99s, errScan)
	}

	fmt.Printf("  (write QPS: %.0f)\n", qpsWrite)
}

// PEBBLE
func runPebbleFull(t *testing.T, n int, dataset, sortedDataset [][2]string) {
	dbPath := filepath.Join(t.TempDir(), "pebble")
	optsWrite := &pebble.Options{
		Cache:                       pebble.NewCache(0),
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 2,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20,
	}

	db, err := pebble.Open(dbPath, optsWrite)
	if err != nil {
		t.Fatalf("Failed to open Pebble: %v", err)
	}

	startWrite := time.Now()
	batch := db.NewBatch()
	for i := 0; i < n; i++ {
		batch.Set([]byte(dataset[i][0]), []byte(dataset[i][1]), nil)
		if i%batchSize == 0 && i > 0 {
			batch.Commit(pebble.Sync)
			batch.Close()
			batch = db.NewBatch()
		}
	}
	batch.Commit(pebble.Sync)
	batch.Close()
	writeDur := time.Since(startWrite)
	qpsWrite := float64(n) / writeDur.Seconds()
	db.Close()

	var actualBytes int64
	err = filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			actualBytes += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk Pebble dir: %v", err)
	}
	cacheBytes := cacheBudgetBytes(actualBytes)
	fmt.Printf("  Pebble cache: %d KB | actual data: %d KB\n", cacheBytes>>10, actualBytes>>10)

	optsRead := &pebble.Options{
		Cache:                       pebble.NewCache(cacheBytes),
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 2,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20,
	}

	db, err = pebble.Open(dbPath, optsRead)
	if err != nil {
		t.Fatalf("Failed to reopen Pebble: %v", err)
	}
	defer db.Close()

	// Cold reads
	latCold := make([]time.Duration, n)
	startCold := time.Now()
	errCold := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, closer, err := db.Get([]byte(dataset[i][0]))
		if err != nil && err != pebble.ErrNotFound {
			errCold++
		}
		if closer != nil {
			closer.Close()
		}
		latCold[i] = time.Since(opStart)
	}
	readDurCold := time.Since(startCold)
	p50c, p95c, p99c := calcPercentiles(latCold)
	printResult("Pebble cold reads", n, readDurCold, p50c, p95c, p99c, errCold)

	// Warm reads
	latWarm := make([]time.Duration, n)
	startWarm := time.Now()
	errWarm := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, closer, err := db.Get([]byte(dataset[i][0]))
		if err != nil && err != pebble.ErrNotFound {
			errWarm++
		}
		if closer != nil {
			closer.Close()
		}
		latWarm[i] = time.Since(opStart)
	}
	readDurWarm := time.Since(startWarm)
	p50w, p95w, p99w := calcPercentiles(latWarm)
	printResult("Pebble warm reads", n, readDurWarm, p50w, p95w, p99w, errWarm)

	// Sequential reads
	latSeq := make([]time.Duration, n)
	startSeq := time.Now()
	errSeq := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, closer, err := db.Get([]byte(sortedDataset[i][0]))
		if err != nil && err != pebble.ErrNotFound {
			errSeq++
		}
		if closer != nil {
			closer.Close()
		}
		latSeq[i] = time.Since(opStart)
	}
	readDurSeq := time.Since(startSeq)
	p50seq, p95seq, p99seq := calcPercentiles(latSeq)
	printResult("Pebble seq reads", n, readDurSeq, p50seq, p95seq, p99seq, errSeq)

	// Range scans
	if n > scanRange {
		scanCount := min(n, 100)
		latScan := make([]time.Duration, scanCount)
		startScan := time.Now()
		errScan := 0
		rng := rand.New(rand.NewSource(12345))
		for i := 0; i < scanCount; i++ {
			startKey, endKey := getRangeKeys(sortedDataset, n, scanRange, rng)
			opStart := time.Now()
			iter, err := db.NewIter(&pebble.IterOptions{
				LowerBound: []byte(startKey),
				UpperBound: []byte(endKey),
			})
			if err != nil {
				errScan++
				continue
			}
			count := 0
			if iter.SeekGE([]byte(startKey)) {
				for ; iter.Valid(); iter.Next() {
					count++
					if count >= scanRange {
						break
					}
				}
			}
			iter.Close()
			latScan[i] = time.Since(opStart)
			if count < scanRange/2 {
				errScan++
			}
		}
		scanDur := time.Since(startScan)
		p50s, p95s, p99s := calcPercentiles(latScan)
		printResult("Pebble range scans", scanCount, scanDur, p50s, p95s, p99s, errScan)
	}

	fmt.Printf("  (write QPS: %.0f)\n", qpsWrite)
}

// Sequential & Range Scan Benchmark (against Pebble only)
func TestSequentialScanBenchmark(t *testing.T) {
	n := 1000000
	fmt.Printf("\n=== Sequential & Range Scan Benchmark (1M keys) ===\n")

	dataset := makeShuffledDataset(n, 12345)
	sortedDataset := make([][2]string, n)
	copy(sortedDataset, dataset)
	sort.Slice(sortedDataset, func(i, j int) bool {
		return sortedDataset[i][0] < sortedDataset[j][0]
	})

	// Fixed cache budget: 20,000 pages = 80 MB (for 4KB pages)
	const cachePages = 20000
	const cacheBytes = cachePages * pageSize

	fmt.Printf("  Cache budget: %d pages (%d MB)\n", cachePages, cacheBytes>>20)

	// Run B+Tree
	runSequentialBPlusTree(t, n, dataset, sortedDataset, cachePages)

	// Run Pebble
	runSequentialPebble(t, n, dataset, sortedDataset, cacheBytes)
}

// B+Tree sequential & range scan runner
func runSequentialBPlusTree(t *testing.T, n int, dataset, sortedDataset [][2]string, cachePages int) {
	dbPath := filepath.Join(t.TempDir(), "btree_seq.db")
	tree := newTree(dbPath, pageSize, order)
	tree.batchsize = batchSize
	tree.pager.maxClean = cachePages

	// Write phase
	startWrite := time.Now()
	for i := 0; i < n; i++ {
		tree.insert(dataset[i][0], dataset[i][1])
	}
	tree.pager.flush()
	writeDur := time.Since(startWrite)
	qpsWrite := float64(n) / writeDur.Seconds()

	// --- Warm-up (sequential read) to populate cache ---
	for i := 0; i < n; i++ {
		tree.search(sortedDataset[i][0])
	}

	// --- Sequential read pass ---
	latSeq := make([]time.Duration, n)
	startSeq := time.Now()
	errSeq := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, found := tree.search(sortedDataset[i][0])
		latSeq[i] = time.Since(opStart)
		if !found {
			errSeq++
		}
	}
	readDurSeq := time.Since(startSeq)
	p50seq, p95seq, p99seq := calcPercentiles(latSeq)
	printResult("B+Tree seq reads", n, readDurSeq, p50seq, p95seq, p99seq, errSeq)

	// --- Range scans (different sizes) ---
	scanSizes := []int{100, 1000, 10000}
	rng := rand.New(rand.NewSource(12345))
	for _, scanSize := range scanSizes {
		if n <= scanSize {
			continue
		}
		scanCount := 100
		latScan := make([]time.Duration, scanCount)
		startScan := time.Now()
		errScan := 0
		for i := 0; i < scanCount; i++ {
			startIdx := rng.Intn(n - scanSize)
			startKey := sortedDataset[startIdx][0]
			endKey := sortedDataset[startIdx+scanSize][0]

			opStart := time.Now()
			keys, vals := tree.scan(startKey, endKey)
			_ = keys
			latScan[i] = time.Since(opStart)
			if len(vals) < scanSize/2 {
				errScan++
			}
		}
		scanDur := time.Since(startScan)
		p50s, p95s, p99s := calcPercentiles(latScan)
		printResult(fmt.Sprintf("B+Tree scan %d", scanSize), scanCount, scanDur, p50s, p95s, p99s, errScan)
	}

	fmt.Printf("  (write QPS: %.0f)\n", qpsWrite)
}

// Pebble sequential & range scan runner
func runSequentialPebble(t *testing.T, n int, dataset, sortedDataset [][2]string, cacheBytes int) {
	dbPath := filepath.Join(t.TempDir(), "pebble_seq.db")
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(int64(cacheBytes)),
		FS:                          nil,
		DisableWAL:                  false,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 2,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20,
		MaxConcurrentCompactions:    nil,
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open Pebble: %v", err)
	}

	// Write
	startWrite := time.Now()
	batch := db.NewBatch()
	for i := 0; i < n; i++ {
		batch.Set([]byte(dataset[i][0]), []byte(dataset[i][1]), nil)
		if i%batchSize == 0 && i > 0 {
			batch.Commit(pebble.Sync)
			batch.Close()
			batch = db.NewBatch()
		}
	}
	batch.Commit(pebble.Sync)
	batch.Close()
	writeDur := time.Since(startWrite)
	qpsWrite := float64(n) / writeDur.Seconds()
	db.Close()

	db, err = pebble.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to reopen Pebble: %v", err)
	}
	defer db.Close()

	// --- Warm-up (sequential read) ---
	for i := 0; i < n; i++ {
		_, closer, _ := db.Get([]byte(sortedDataset[i][0]))
		if closer != nil {
			closer.Close()
		}
	}

	// --- Sequential read pass ---
	latSeq := make([]time.Duration, n)
	startSeq := time.Now()
	errSeq := 0
	for i := 0; i < n; i++ {
		opStart := time.Now()
		_, closer, err := db.Get([]byte(sortedDataset[i][0]))
		if err != nil && err != pebble.ErrNotFound {
			errSeq++
		}
		if closer != nil {
			closer.Close()
		}
		latSeq[i] = time.Since(opStart)
	}
	readDurSeq := time.Since(startSeq)
	p50seq, p95seq, p99seq := calcPercentiles(latSeq)
	printResult("Pebble seq reads", n, readDurSeq, p50seq, p95seq, p99seq, errSeq)

	// --- Range scans ---
	scanSizes := []int{100, 1000, 10000}
	rng := rand.New(rand.NewSource(12345))
	for _, scanSize := range scanSizes {
		if n <= scanSize {
			continue
		}
		scanCount := 100
		latScan := make([]time.Duration, scanCount)
		startScan := time.Now()
		errScan := 0
		for i := 0; i < scanCount; i++ {
			startIdx := rng.Intn(n - scanSize)
			startKey := sortedDataset[startIdx][0]
			endKey := sortedDataset[startIdx+scanSize][0]

			opStart := time.Now()
			iter, err := db.NewIter(&pebble.IterOptions{
				LowerBound: []byte(startKey),
				UpperBound: []byte(endKey),
			})
			if err != nil {
				errScan++
				continue
			}
			count := 0
			if iter.SeekGE([]byte(startKey)) {
				for ; iter.Valid(); iter.Next() {
					count++
					if count >= scanSize {
						break
					}
				}
			}
			iter.Close()
			latScan[i] = time.Since(opStart)
			if count < scanSize/2 {
				errScan++
			}
		}
		scanDur := time.Since(startScan)
		p50s, p95s, p99s := calcPercentiles(latScan)
		printResult(fmt.Sprintf("Pebble scan %d", scanSize), scanCount, scanDur, p50s, p95s, p99s, errScan)
	}

	fmt.Printf("  (write QPS: %.0f)\n", qpsWrite)
}
