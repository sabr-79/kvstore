package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"slices"
	"sync"
	"time"
)

type KVstore struct {
	mu   sync.RWMutex
	dict map[string]string
}

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

type RaftNode struct {
	mu              sync.Mutex
	currentTerm     int
	votedFor        string
	votesReceived   int
	log             []int
	role            Role
	id              string
	peers           []string
	timeout         int
	stateMachine    *KVstore
	commitIndex     int
	lastApplied     int
	nextIndex       map[string]int
	matchIndex      map[string]int
	electionResetCh chan struct{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId string
}

type ReplyEntriesArgs struct {
	Term    int
	Success bool
}

func newRaftNode(id string, peers []string) *RaftNode {
	newNode := RaftNode{role: Follower, currentTerm: 0, votedFor: "", id: id, peers: peers, electionResetCh: make(chan struct{}, 1)}
	go electionTimer(&newNode)
	return &newNode

}

func requestVoteHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "not allowed bro", http.StatusBadRequest)
			return
		}

		var args RequestVoteArgs
		err := json.NewDecoder(r.Body).Decode(&args)
		if err != nil {
			http.Error(w, "not valid", http.StatusBadRequest)
			return
		}
		reply := requestVote(node, args)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(reply)

	}
}

func sendVoteRequest(peerAddr string, args RequestVoteArgs) (RequestVoteReply, error) {
	body, err := json.Marshal(args)
	if err != nil {
		return RequestVoteReply{}, err
	}

	resp, err := http.Post("http://"+peerAddr+"/request-vote", "application/json", bytes.NewReader(body))
	if err != nil {
		return RequestVoteReply{}, err
	}
	var reply RequestVoteReply
	err = json.NewDecoder(resp.Body).Decode(&reply)
	defer resp.Body.Close()

	if err != nil {
		return RequestVoteReply{}, err
	}
	return reply, err

}

func appendEntriesHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "not allowed bro", http.StatusBadRequest)
			return
		}

		var args AppendEntriesArgs
		err := json.NewDecoder(r.Body).Decode(&args)
		if err != nil {
			http.Error(w, "not valid", http.StatusBadRequest)
			return
		}
		reply := appendEntries(node, args)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(reply)

	}
}
func sendAppendedEntry(peerAddr string, arg AppendEntriesArgs) (ReplyEntriesArgs, error) {
	body, err := json.Marshal(arg)
	if err != nil {
		return ReplyEntriesArgs{}, err

	}

	resp, err := http.Post("http://"+peerAddr+"/append-entry", "application/json", bytes.NewReader(body))
	if err != nil {
		return ReplyEntriesArgs{}, err

	}
	defer resp.Body.Close()
	var reply ReplyEntriesArgs
	err = json.NewDecoder(resp.Body).Decode(&reply)
	if err != nil {
		return ReplyEntriesArgs{}, err

	}
	return reply, nil
}

func electionTimer(node *RaftNode) {
	for {
		randomDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
		select {
		case <-time.After(randomDuration):
			node.mu.Lock()
			isLeader := node.role
			node.mu.Unlock()

			if isLeader != Leader {
				becomeCandidate(node)
			}

		case <-node.electionResetCh:

		}
	}

}

func runHeartbeats(node *RaftNode, _ AppendEntriesArgs) {
	node.mu.Lock()
	if node.role != Leader {
		node.mu.Unlock()
		return
	}

	node.mu.Unlock()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		node.mu.Lock()
		term := node.currentTerm
		isLeader := node.role
		id := node.id
		node.mu.Unlock()
		if isLeader != Leader {
			return
		}
		newArg := AppendEntriesArgs{Term: term, LeaderId: id}
		for _, peer := range node.peers {
			go func(p string) {
				reply, err := sendAppendedEntry(p, newArg)
				if err != nil {
					return
				}
				node.mu.Lock()
				if reply.Term > node.currentTerm {
					node.role = Follower
					node.currentTerm = reply.Term
					node.votedFor = ""
				}
				select {
				case node.electionResetCh <- struct{}{}:
				default:
				}
				node.mu.Unlock()
			}(peer)

		}

	}

}

func becomeCandidate(node *RaftNode) {
	node.mu.Lock()
	node.currentTerm += 1
	node.votedFor = node.id
	node.votesReceived = 1
	node.role = Candidate

	var args = RequestVoteArgs{Term: node.currentTerm, CandidateId: node.id, LastLogIndex: node.commitIndex, LastLogTerm: node.lastApplied}
	var replyCh = make(chan RequestVoteReply, len(node.peers))
	node.mu.Unlock()
	for _, peer := range node.peers {
		go func(p string) {
			reply, err := sendVoteRequest(p, args)
			if err != nil {
				return
			}
			replyCh <- reply
		}(peer)
	}
	votes := 1
	node.mu.Lock()
	term := node.currentTerm
	node.mu.Unlock()
	for i := 0; i < len(node.peers); i++ {
		reply := <-replyCh
		node.mu.Lock()
		if reply.Term > node.currentTerm {
			node.role = Follower
			node.currentTerm = reply.Term
			node.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			votes++
		}
		candidacy := node.role == Candidate && node.currentTerm == term
		node.mu.Unlock()
		if !candidacy {
			return
		}
		if votes > len(node.peers)/2 {
			becomeLeader(node)
			return
		}

	}

}
func requestVote(node *RaftNode, arg RequestVoteArgs) RequestVoteReply {
	node.mu.Lock()
	voteGranted := false
	if arg.Term < node.currentTerm {
		reply := RequestVoteReply{Term: node.currentTerm, VoteGranted: voteGranted}
		node.mu.Unlock()
		return reply
	}
	if arg.Term > node.currentTerm {
		node.role = Follower
		node.currentTerm = arg.Term
		node.votedFor = ""
	}
	if node.votedFor == "" || node.votedFor == arg.CandidateId {
		voteGranted = true
		node.votedFor = arg.CandidateId
		select {
		case node.electionResetCh <- struct{}{}:
		default:
		}

	}

	reply := RequestVoteReply{Term: node.currentTerm, VoteGranted: voteGranted}
	node.mu.Unlock()
	return reply
}

func becomeLeader(node *RaftNode) {
	node.mu.Lock()
	node.role = Leader
	var arg = AppendEntriesArgs{Term: node.currentTerm, LeaderId: node.id}
	node.mu.Unlock()
	go runHeartbeats(node, arg)

}

func appendEntries(node *RaftNode, arg AppendEntriesArgs) ReplyEntriesArgs {
	node.mu.Lock()
	defer node.mu.Unlock()
	if arg.Term < node.currentTerm {
		return ReplyEntriesArgs{Term: node.currentTerm, Success: false}

	} else {
		select {
		case node.electionResetCh <- struct{}{}:
		default:
		}
		if arg.Term > node.currentTerm {
			node.role = Follower
			node.votedFor = ""
			node.currentTerm = arg.Term
		}

	}
	return ReplyEntriesArgs{Term: node.currentTerm, Success: true}
}

func get(store *KVstore, key string) (string, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.dict == nil {
		store.dict = make(map[string]string)
	}
	val, ok := store.dict[key]
	if ok {
		return val, ok
	}
	return "", ok

}
func getHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		val, valid := get(store, key)

		if !valid {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no such key exists"})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			Message string `json:"message"`
			Key     string `json:"key"`
			Value   string `json:"value"`
		}{
			Message: "Obtained key",
			Key:     key,
			Value:   val,
		})

	}

}
func put(store *KVstore, key string, val string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.dict == nil {
		store.dict = make(map[string]string)
	}
	store.dict[key] = val
}
func putHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")

		if key == "" || val == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		put(store, key, val)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			Message string `json:"message"`
			Key     string `json:"key"`
			Value   string `json:"value"`
		}{
			Message: "Stored key",
			Key:     key,
			Value:   val,
		})

	}

}

func remove(store *KVstore, key string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.dict == nil {
		store.dict = make(map[string]string)
	}

	_, ok := store.dict[key]
	if ok {
		delete(store.dict, key)

	}
	return ok

}

func removeHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		ok := remove(store, key)
		if ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(struct {
				Message string `json:"message"`
				Key     string `json:"key"`
			}{
				Message: "Removed key",
				Key:     key,
			})

		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no such key exists"})
			return

		}

	}

}
func Scan(store *KVstore, startKey string, endKey string) map[string]string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	keys := []string{}
	for k := range store.dict {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	res := map[string]string{}

	for _, x := range keys {
		if x >= startKey && x <= endKey {
			res[x] = store.dict[x]

		}

	}
	return res

}
func scanHandler(store *KVstore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		skey := r.URL.Query().Get("startKey")
		ekey := r.URL.Query().Get("endKey")

		if skey == "" || ekey == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		res := Scan(store, skey, ekey)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			Message  string            `json:"message"`
			StartKey string            `json:"startkey"`
			EndKey   string            `json:"endkey"`
			Result   map[string]string `json:"result"`
		}{
			Message:  "Scanned keys",
			StartKey: skey,
			EndKey:   ekey,
			Result:   res,
		})

	}

}

func main() {
	mux := http.NewServeMux()
	myStore := KVstore{}
	put(&myStore, "7", "9")
	x, _ := get(&myStore, "7")
	y, _ := get(&myStore, "1")
	z := remove(&myStore, "9")
	w := Scan(&myStore, "0", "7")
	fmt.Println(w)
	fmt.Println(x)
	fmt.Println(y)
	fmt.Println(z)
	mux.HandleFunc("/get", getHandler(&myStore))
	mux.HandleFunc("/put", putHandler(&myStore))
	mux.HandleFunc("/remove", removeHandler(&myStore))
	mux.HandleFunc("/scan", scanHandler(&myStore))
	fmt.Println("Server starting on :8080...")
	http.ListenAndServe(":8080", mux)

}
