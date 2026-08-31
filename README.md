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

Before split opt, 1M keys:

B+Tree cache: 10644 KB | actual data: 90864 KB
B+Tree warm reads      | 1000000 ops |    268557 qps | p50=3.833µs | p95=7.667µs | p99=12.833µs | errs=0
B+Tree seq reads       | 1000000 ops |   3631085 qps | p50=  167ns | p95=  209ns | p99=3.208µs | errs=0
B+Tree cold reads      | 1000000 ops |     85557 qps | p50=10.125µs | p95=15.958µs | p99=23.625µs | errs=0
B+Tree range scans     |     100 ops |     46418 qps | p50=17.583µs | p95=36.583µs | p99=111.125µs | errs=0
(write QPS: 29686)

After split opt, 1M keys:

B+Tree cache: 10644 KB | actual data: 45700 KB
B+Tree warm reads      | 1000000 ops |    297560 qps | p50=3.667µs | p95=7.583µs | p99=10.875µs | errs=0
B+Tree seq reads       | 1000000 ops |   3864475 qps | p50=  167ns | p95=  209ns | p99=1.792µs | errs=0
B+Tree cold reads      | 1000000 ops |     87741 qps | p50=9.875µs | p95=15.25µs | p99=20.625µs | errs=0
B+Tree range scans     |     100 ops |     57133 qps | p50=15.417µs | p95=25.709µs | p99=37.459µs | errs=0
(write QPS: 44674)

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
