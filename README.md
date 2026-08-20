# Distributed KV Store (Raft, B+ tree, Go)
 
A distributed, Raft-replicated key-value store built from scratch, with a live cluster-state visualizer, chaos testing, and a B+tree storage engine.

Currently, it's a single node KV store using Go's map structure and net/http, with plans to implement
the B+ tree storage engine once the Raft logic is complete. 

 
 
### What's done (Phase 1 — single-node KV store)
- In-memory store (`map[string]string`) behind `sync.RWMutex`
- HTTP+JSON API: `GET /get`, `PUT/POST /put`, `DELETE /remove`, `GET /scan`
- `Get`/`Put`/`Remove`/`Scan` all implemented
- Concurrency-tested under `go test -race`, including a stress test hammering a single contested key from 100 goroutines to verify no torn/corrupted writes
- Implemented raft-style elections and tested it in `main_test.go`

### Up next (Phase 3 — Raft: log replicatioin)
-  Enable AppendEntries to carry meaningful entries
-  Apply commited entries to the KVStore
-  Redirect follower writes to the current leader

 
## Roadmap overview
 
|Phase | Focus                 | Status      |
|------|-----------------------|-------------|
| 0    | Go/setup fundamentals | Done        |
| 1    | Single-node KV store  | Done        |
| 2    | Raft: leader election | Done        |
| 3    | Raft: log replication | Not started |
| 4    | Raft log persistence  | Not started |
| 5    | B+tree storage engine | Not started |
| 6    | Snapshotting          | Not started |
| 7    | Live state visualizer | Not started |
| 8    | Chaos testing         | Not started |
| 9    | Benchmarking          | Not started |

## Design notes
- **Transport:** HTTP+JSON for inter-node RPCs for now, may change later (gRPC) for optimization
- **Storage engine:** B+ tree from scratch for read optimization (instead of write) and range scans

