package main

import (
	"slices"
	"strings"
	"sync"
	"time"
)

type KVstore struct {
	mu   sync.RWMutex
	dict map[string]string
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

func put(store *KVstore, key string, val string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.dict == nil {
		store.dict = make(map[string]string)
	}
	store.dict[key] = val
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
