# Distributed KV Store (Raft, B+ tree, Go)
 
A distributed, Raft-replicated key-value store built from scratch, with a planned live cluster-state visualizer and chaos testing.

Currently, it's a distributive KV store using a B+ tree as a storage engine and net/http for the RPC. 

 
 
### What's done 
- HTTP+JSON API: `GET /get`, `PUT/POST /put`, `DELETE /remove`, `GET /scan`
- `Get`/`Put`/`Remove`/`Scan` all implemented
- Concurrency-tested under `go test -race`, including a stress test hammering a single contested key from 100 goroutines to verify no torn/corrupted writes
- Implemented raft-style elections and tested it in `main_test.go`
- Implemented log replication, persistence
- Built the storage engine, replaced and benchmarked it against SQLite and PebbleDB
- Implemented snapshotting for log compaction

### Up next 
- Live state visualizer showcasing the current system



## Roadmap overview
 
|Phase | Focus                 | Status      |
|------|-----------------------|-------------|
| 0    | Go/setup fundamentals | Done        |
| 1    | Single-node KV store  | Done        |
| 2    | Raft: leader election | Done        |
| 3    | Raft: log replication | Done        |
| 4    | Raft log persistence  | Done        |
| 5    | B+ tree storage engine| Done        |
| 6    | Snapshotting          | Done        |
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
├── main_test.go       # Testing for current phase (raft, btree validation, etc)
├── main.go            # Web server entrypoint
├── persistence.go     # Reading/writing from disk for both Raft logs and tree data
├── raft.go            # RaftNode struct, state changes, election, and heartbeats
├── rpc.go             # Outbound network helpers 
├── snapshot.go        # Stop the world snapshot functions
└── storage.go         # KVstore struct, core operations + log application loop
```

## Current Benchmarking:
- Tested the storage engine against SQLite (uses B/B+ trees) and PebbleDB (LSM, also written in Go). 
- Lost on writes against both databases, but B+ Trees are more optimized for reads rather than writes
- Won sequential and warm reads by a large margin due to slotted page layout and cache locality
- However PebbleDB and SQLite beat the B+ Tree at cold reads at 1M keys

## Benchmark Comparison (1M keys, equal memory budget)

| Workload          | B+Tree            | SQLite      | Pebble      |
|-------------------|-------------------|-------------|-------------|
| Warm point reads  | **226,334 QPS**   | 127,173 QPS | 139,416 QPS |
| Sequential reads  | **3,992,074 QPS** | 123,931 QPS | 143,686 QPS |
| Cold point reads  | 91,051 QPS        | 123,857 QPS | 136,278 QPS |
| Range scans       | 48,470 QPS        | 5,317 QPS   | 70,920 QPS  |
| Writes            | 27,022 QPS        | 55,248 QPS  | 337,092 QPS |
| On‑disk footprint | 45,708 KB         | 50,812 KB   | 24,250 KB   |

## Raft + B+ Tree, no snapshotting, 20k ops
| Workload    | QPS        | p50 latency | p99 latency |
|-------------|------------|-------------|-------------|
| PUT         | 3003 QPS   | 5.288666ms  | 13.101334ms |
| GET         | 18000 QPS  | 49.75µs     | 139.458µs   |


## Design notes
- **Transport:** HTTP+JSON for inter-node RPCs for now, may change later (gRPC) for optimization
- **Storage engine:** B+ tree from scratch for read optimization (instead of write) and range scans
- **Slotted Pages:**  Fixed size pages with variable length key/vals, looking into prefix compression as an optimzation
- **Big Endian byte encoding:** Although my device uses little endian, big endian provides natural sort order at the virtually non existent cost of a single clock cycle to flip them.
- **F_FULLFSYNC vs fsync**: Used an F_FULLSYNC  ensure absolute crash safety on a MacOS device, since fsync may allow some data to remain in the volatile cache
- **LRU Cache**: Currently stores recently read decoded pages to speed up reads
- **Minimal Deletion**: Like other databases, we're tolerating underflow to keep deletion overhead small
- **Crash Safety**: To make the snapshotting process easier, I chose to implement copy on write for persistence
- **DFS vs Linked List**: Since using a leaf linked list for range scans would cause heavy write amplification with copy on write, we can use an optimized DFS approach instead. It's somewhat slower than my previous linked list implementation w/o copy on write. (48,470 QPS vs 57,133 QPS)
- **Free-list page allocation:** Implemented to bound file growth. 
- **Known limitations**: Since page sizes are 4KB, you can only insert key/val pairs smaller than the total page size. Likewise, the freed page list can overflow the metadata page with heavy writes/deletes. Planning on implementing multi page storage for large key/val pairs and refractoring freelist as a linked list. 
