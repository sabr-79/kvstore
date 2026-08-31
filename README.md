# Distributed KV Store (Raft, B+ tree, Go)
 
A distributed, Raft-replicated key-value store built from scratch, with a planned live cluster-state visualizer, chaos testing, and a B+tree storage engine.

Currently, it's a distributive KV store using Go's map structure and net/http. The B+ Tree is completed and is in the process of connecting to the raft sytem.

 
 
### What's done 
- In-memory store (`map[string]string`) behind `sync.RWMutex`
- HTTP+JSON API: `GET /get`, `PUT/POST /put`, `DELETE /remove`, `GET /scan`
- `Get`/`Put`/`Remove`/`Scan` all implemented
- Concurrency-tested under `go test -race`, including a stress test hammering a single contested key from 100 goroutines to verify no torn/corrupted writes
- Implemented raft-style elections and tested it in `main_test.go`
- Implemented log replication, persistence
- Built the storage engine and benchmarked it against SQLite and PebbleDB

### Up next 
-  Wire the B+ tree in before working on snapshotting



## Roadmap overview
 
|Phase | Focus                 | Status      |
|------|-----------------------|-------------|
| 0    | Go/setup fundamentals | Done        |
| 1    | Single-node KV store  | Done        |
| 2    | Raft: leader election | Done        |
| 3    | Raft: log replication | Done        |
| 4    | Raft log persistence  | Done        |
| 5    | B+ tree storage engine| Done        |
| 6    | Snapshotting          | Not started |
| 7    | Live state visualizer | Not started |
| 8    | Chaos testing         | Not started |
| 9    | Benchmarking          | In progress |

## File Structure
```
kv/
├── benchmark_test.go  # Testing against SQLite and PebbleDB over various read and write tasks
├── benchmark.txt      # Detailed benchmark results
├── btree.go           # Storage engine
├── handlers.go        # Inbound network helpers
├── main_test.go       # Testing for current phase (raft, btree validation)
├── main.go            # Web server entrypoint
├── persistence.go     # Reading/writing from disk
├── raft.go            # RaftNode struct, state changes, election, and heartbeats
├── rpc.go             # Outbound network helpers 
└── storage.go         # KVstore struct, core operations + log application loop
```

## Current Benchmarking:
- Tested the storage engine against SQLite (uses B/B+ trees) and PebbleDB (LSM, also written in Go). 
- Lost on writes against both databases, but B+ Trees are more optimized for reads rather than writes
- Won sequential reads by a large margin due to leaf linked pages and locality
- Won at warm reads and range scans against both up to 10,000 keys
- However PebbleDB beat the B+ Tree at range scans and cold reads at 1M keys
- The B+ Tree beat SQLite on all metrics at 1M keys save for writes and cold reads
- Instead of splitting by only key count, we can split by byte count as well to improve storage and overall speed.

| Metric                | Before              | After              | %           |
|-----------------------|---------------------|--------------------|-------------|
| On‑disk footprint     | 90,864 KB           | 45,700 KB          | **−50%**    |
| Warm point reads      | 268,557 QPS         | 297,560 QPS        | **+10.8%**  |
| Sequential reads      | 3,631,085 QPS       | 3,864,475 QPS      | **+6.4%**   |
| Range scans           | 46,418 QPS          | 57,133 QPS         | **+23%**    |
| Durable writes        | 29,686 QPS          | 44,674 QPS         | **+50%**    |


## Benchmark Comparison (1M keys, equal memory budget)

| Workload          | B+Tree            | SQLite      | Pebble      |
|-------------------|-------------------|-------------|-------------|
| Warm point reads  | **297,560 QPS**   | 127,173 QPS | 139,416 QPS |
| Sequential reads  | **3,864,475 QPS** | 123,931 QPS | 143,686 QPS |
| Cold point reads  | 87,741 QPS        | 123,857 QPS | 136,278 QPS |
| Range scans       | 57,133 QPS        | 5,317 QPS   | 70,920 QPS  |
| Durable writes    | 44,674 QPS        | 55,248 QPS  | 337,092 QPS |
| On‑disk footprint | 45,700 KB         | 50,812 KB   | 24,250 KB   |


## Design notes
- **Transport:** HTTP+JSON for inter-node RPCs for now, may change later (gRPC) for optimization
- **Storage engine:** B+ tree from scratch for read optimization (instead of write) and range scans
- **Slotted Pages:**  Fixed size pages with variable length key/vals, looking into prefix compression as an optimzation
- **Big Endian byte encoding:** Although my device uses little endian, big endian provides natural sort order at the virtually non existent cost of a single clock cycle to flip them.
- **F_FULLFSYNC vs fsync**: Used an F_FULLSYNC  ensure absolute crash safety on a MacOS device, since fsync may allow some data to remain in the volatile cache
- **LRU Cache**: Currently stores recently read decoded pages to speed up reads
- **Minimal Deletion**: Like other databases, we're tolerating underflow to keep deletion overhead small
- **Crash Safety**: To make the snapshotting process easier, I chose to implement copy on write for persistence
- **Free-list page allocation:** Implemented to bound file growth. 
- **Known limitations**: Since page sizes are 4KB, you can only insert key/val pairs smaller than the total page size. Likewise, the freed page list can overflow the metadata page with heavy writes/deletes. Planning on implementing multi page storage for large key/val pairs and refractoring freelist as a linked list. 
