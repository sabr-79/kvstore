package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

type KVstore struct {
	mu   sync.RWMutex
	dict map[string]string
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
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type ReplyEntriesArgs struct {
	Term    int
	Success bool
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

		for _, peer := range node.peers {
			node.mu.Lock()
			nxt := node.nextIndex[peer]
			prevIdx := nxt - 1
			var prevTerm int
			if prevIdx == 0 {
				prevTerm = 0
			} else {
				prevTerm = node.log[prevIdx-1].Term
			}
			entriesToSend := node.log[prevIdx:]
			newArg := AppendEntriesArgs{Term: term, LeaderId: id, PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, Entries: entriesToSend, LeaderCommit: node.commitIndex}
			node.mu.Unlock()
			go func(p string, args AppendEntriesArgs) {
				reply, err := sendAppendedEntry(p, args)
				if err != nil {
					return
				}
				node.mu.Lock()
				if reply.Term > node.currentTerm {
					node.role = Follower
					node.currentTerm = reply.Term
					node.votedFor = ""
				}
				if reply.Success {
					node.matchIndex[peer] = prevIdx + len(entriesToSend)
					node.nextIndex[peer] = node.matchIndex[peer] + 1

					for i := node.commitIndex + 1; i <= len(node.log); i++ {
						var count = 1
						for _, peer := range node.peers {
							if node.matchIndex[peer] >= i {
								count++
							}

						}
						if count >= (len(node.peers)+1)/2 {
							if node.log[i-1].Term == node.currentTerm {
								node.commitIndex = i
							}
						}

					}

				}
				if !reply.Success {
					if node.nextIndex[peer] > 1 {
						node.nextIndex[peer]--

					}

				}
				select {
				case node.electionResetCh <- struct{}{}:
				default:
				}
				node.mu.Unlock()
			}(peer, newArg)

		}

	}

}

func becomeCandidate(node *RaftNode) {
	node.mu.Lock()
	node.currentTerm += 1
	node.votedFor = node.id
	node.votesReceived = 1
	node.role = Candidate

	lastIdx := len(node.log)
	lastTerm := 0
	if lastIdx > 0 {
		lastTerm = node.log[lastIdx-1].Term
	}
	var args = RequestVoteArgs{Term: node.currentTerm, CandidateId: node.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
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
	node.nextIndex = make(map[string]int)
	node.matchIndex = make(map[string]int)
	for _, peer := range node.peers {
		node.nextIndex[peer] = len(node.log) + 1
		node.matchIndex[peer] = 0
	}
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
		if len(node.log) < arg.PrevLogIndex {
			return ReplyEntriesArgs{Term: node.currentTerm, Success: false}
		}
		if arg.PrevLogIndex > 0 && node.log[arg.PrevLogIndex-1].Term != arg.PrevLogTerm {
			return ReplyEntriesArgs{Term: node.currentTerm, Success: false}
		}

		if arg.Term > node.currentTerm {
			node.role = Follower
			node.votedFor = ""
			node.currentTerm = arg.Term
			node.leaderId = arg.LeaderId

		} else if arg.Term == node.currentTerm && node.role != Follower {
			node.role = Follower
			node.leaderId = arg.LeaderId
		}
		for _, entry := range arg.Entries {
			matchEntry := entry.Index - 1

			if matchEntry < len(node.log) {
				if entry.Term != node.log[matchEntry].Term || node.log[matchEntry].Command != entry.Command {
					node.log = node.log[:matchEntry]
				}

			}

			if len(node.log) == matchEntry {
				node.log = append(node.log, entry)
			}

		}
		if arg.LeaderCommit > node.commitIndex {
			node.commitIndex = min(arg.LeaderCommit, len(node.log))
		}

	}
	return ReplyEntriesArgs{Term: node.currentTerm, Success: true}
}
func applyLoop(node *RaftNode) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		node.mu.Lock()
		for node.commitIndex > node.lastApplied {
			node.lastApplied++
			entry := node.log[node.lastApplied-1]
			commands := strings.Split(entry.Command, ":")
			switch commands[0] {
			case "PUT":
				put(node.stateMachine, commands[1], commands[2])
			case "REMOVE":
				remove(node.stateMachine, commands[1])
			}

		}

		node.mu.Unlock()

	}

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
func getHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		node.mu.Lock()
		isLeader := node.role == Leader
		leaderId := node.leaderId
		if !isLeader {
			node.mu.Unlock()
			http.Redirect(w, r, "http://"+leaderId+r.URL.Path+"?"+r.URL.RawQuery, http.StatusSeeOther)
			return

		}
		if r.Method != http.MethodGet {
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			node.mu.Unlock()
			return
		}
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			node.mu.Unlock()
			return
		}
		val, valid := get(node.stateMachine, key)

		if !valid {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no such key exists"})
			node.mu.Unlock()
			return
		}

		node.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "Got key", "key": key, "value": val})

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
func putHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		node.mu.Lock()
		isLeader := node.role == Leader
		leaderId := node.leaderId
		if !isLeader {
			node.mu.Unlock()
			http.Redirect(w, r, "http://"+leaderId+r.URL.Path+"?"+r.URL.RawQuery, http.StatusSeeOther)
			return

		}
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			node.mu.Unlock()
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")

		if key == "" || val == "" {
			http.Error(w, "invalid param", http.StatusBadRequest)
			node.mu.Unlock()
			return
		}
		//put(node.stateMachine, key, val)
		targetIdx := len(node.log) + 1

		var logEntry = LogEntry{Index: len(node.log) + 1, Term: node.currentTerm, Command: "PUT:" + key + ":" + val}
		node.log = append(node.log, logEntry)

		for node.commitIndex < targetIdx && node.role == Leader {
			node.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			node.mu.Lock()
		}

		node.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "Stored key", "key": key, "value": val})

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

func removeHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		node.mu.Lock()
		isLeader := node.role == Leader
		leaderId := node.leaderId
		if !isLeader {
			node.mu.Unlock()
			http.Redirect(w, r, "http://"+leaderId+r.URL.Path+"?"+r.URL.RawQuery, http.StatusSeeOther)
			return

		}
		if r.Method != http.MethodDelete {
			node.mu.Unlock()
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")

		if key == "" {
			node.mu.Unlock()
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		//ok := remove(node.stateMachine, key)

		//if ok {
		targetIdx := len(node.log) + 1

		var logEntry = LogEntry{Index: len(node.log) + 1, Term: node.currentTerm, Command: "REMOVE:" + key}
		node.log = append(node.log, logEntry)

		for node.commitIndex < targetIdx && node.role == Leader {
			node.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			node.mu.Lock()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "Removed key", "key": key})
		node.mu.Unlock()

		// w.Header().Set("Content-Type", "application/json")
		// w.WriteHeader(http.StatusNotFound)
		//json.NewEncoder(w).Encode(map[string]string{"error": "no such key exists"})
		//node.mu.Unlock()

	}

}

func scan(store *KVstore, startKey string, endKey string) map[string]string {
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
func scanHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		node.mu.Lock()
		isLeader := node.role == Leader
		leaderId := node.leaderId
		if !isLeader {
			node.mu.Unlock()
			http.Redirect(w, r, "http://"+leaderId+r.URL.Path+"?"+r.URL.RawQuery, http.StatusSeeOther)
			return

		}
		if r.Method != http.MethodGet {
			node.mu.Unlock()
			http.Error(w, "not allowed bro", http.StatusMethodNotAllowed)
			return
		}
		skey := r.URL.Query().Get("startKey")
		ekey := r.URL.Query().Get("endKey")

		if skey == "" || ekey == "" {
			node.mu.Unlock()
			http.Error(w, "invalid param", http.StatusBadRequest)
			return
		}
		res := scan(node.stateMachine, skey, ekey)
		node.mu.Unlock()

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
	// myStore := KVstore{}
	// put(&myStore, "7", "9")
	// x, _ := get(&myStore, "7")
	// y, _ := get(&myStore, "1")
	// z := remove(&myStore, "9")
	w := 7
	fmt.Println(w)
	// fmt.Println(x)
	// fmt.Println(y)
	// fmt.Println(z)
	// mux.HandleFunc("/get", getHandler(&myStore))
	// mux.HandleFunc("/put", putHandler(&myStore))
	// mux.HandleFunc("/remove", removeHandler(&myStore))
	// mux.HandleFunc("/scan", scanHandler(&myStore))
	// fmt.Println("Server starting on :8080...")
	http.ListenAndServe(":8080", mux)

}
