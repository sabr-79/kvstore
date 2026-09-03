package main

import (
	"encoding/json"
	"net/http"
	"time"
)

func requestVoteHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if node.isDead {
			http.Error(w, "definitely not allowed lil bro", http.StatusServiceUnavailable)
			return
		}
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

func appendEntriesHandler(node *RaftNode) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		if node.isDead {
			http.Error(w, "definitely not allowed lil bro", http.StatusServiceUnavailable)
			return
		}
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
		node.mu.Unlock()

		val, valid := get(node.stateMachine, key)

		if !valid {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "no such key exists"})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "Got key", "key": key, "value": val})

	}

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
		targetIdx := node.snapIndex + len(node.log) + 1
		var logEntry = LogEntry{Index: targetIdx, Term: node.currentTerm, Command: "PUT:" + key + ":" + val}
		node.log = append(node.log, logEntry)
		node.pendingLog = append(node.pendingLog, logEntry)
		if len(node.pendingLog) >= node.batchsize {
			node.mu.Unlock()
			persistLog(node)
			node.mu.Lock()
		}

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

		targetIdx := node.snapIndex + len(node.log) + 1
		var logEntry = LogEntry{Index: targetIdx, Term: node.currentTerm, Command: "REMOVE:" + key}
		node.log = append(node.log, logEntry)
		node.pendingLog = append(node.pendingLog, logEntry)
		if len(node.pendingLog) >= node.batchsize {
			node.mu.Unlock()
			persistLog(node)
			node.mu.Lock()
		}

		for node.commitIndex < targetIdx && node.role == Leader {
			node.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			node.mu.Lock()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "Removed key", "key": key})
		node.mu.Unlock()

	}

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
		node.mu.Unlock()

		resKey, resVal := scan(node.stateMachine, skey, ekey)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			Message  string   `json:"message"`
			StartKey string   `json:"startkey"`
			EndKey   string   `json:"endkey"`
			ResultK  []string `json:"resultK"`
			ResultV  []string `json:"resultV"`
		}{
			Message:  "Scanned keys",
			StartKey: skey,
			EndKey:   ekey,
			ResultK:  resKey,
			ResultV:  resVal,
		})

	}

}

func installSnapshotHandler(node *RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var args InstallSnapshotArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		reply := installSnapshot(node, args)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(reply)
	}
}
