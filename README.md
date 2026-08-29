# Distributed KV Store (Raft, B+ tree, Go)
 
A distributed, Raft-replicated key-value store built from scratch, with a live cluster-state visualizer, chaos testing, and a B+tree storage engine.

Currently, it's a single node KV store using Go's map structure and net/http, with plans to implement
the B+ tree storage engine once the Raft logic is complete. 

 
 
### What's done 
- In-memory store (`map[string]string`) behind `sync.RWMutex`
- HTTP+JSON API: `GET /get`, `PUT/POST /put`, `DELETE /remove`, `GET /scan`
- `Get`/`Put`/`Remove`/`Scan` all implemented
- Concurrency-tested under `go test -race`, including a stress test hammering a single contested key from 100 goroutines to verify no torn/corrupted writes
- Implemented raft-style elections and tested it in `main_test.go`
- Implemented log replication, persistence

### Up next (Phase 5)
-  Switch to using a B+ tree over go's map structure
- Implement Page Allocation/freeing, copy on write
- Explore caching and other optimization techniques to improve writes and internal fragmentation



## Roadmap overview
 
|Phase | Focus                 | Status      |
|------|-----------------------|-------------|
| 0    | Go/setup fundamentals | Done        |
| 1    | Single-node KV store  | Done        |
| 2    | Raft: leader election | Done        |
| 3    | Raft: log replication | Done        |
| 4    | Raft log persistence  | Done        |
| 5    | B+ tree storage engine| Not started |
| 6    | Snapshotting          | Not started |
| 7    | Live state visualizer | Not started |
| 8    | Chaos testing         | Not started |
| 9    | Benchmarking          | Not started |

## File Structure
```
kv/
├── handlers.go        # Inbound network helpers
├── main_test.go       # Testing for current phase
├── main.go            # Web server entrypoint
├── persistence.go     # Reading/writing from disk
├── raft.go            # RaftNode struct, state changes, election, and heartbeats
├── rpc.go             # Outbound network helpers 
└── storage.go         # KVstore struct, core operations + log application loop
```

## Design notes
- **Transport:** HTTP+JSON for inter-node RPCs for now, may change later (gRPC) for optimization
- **Storage engine:** B+ tree from scratch for read optimization (instead of write) and range scans
- **Slotted Pages:**  For speedy lookups, looking into prefix compression as an optimization
- **Big Endian byte encoding:** Although my device uses little endian, big endian provides natural sort order at the virtually non existent cost of a single clock cycle to flip them
