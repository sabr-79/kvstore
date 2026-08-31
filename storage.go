package main

import (
	"strings"
	"sync"
	"time"
)

type KVstore struct {
	mu   sync.RWMutex
	tree *TreeRoot
}

func get(store *KVstore, key string) (string, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	val, ok := store.tree.search(key)
	return val, ok

}

func put(store *KVstore, key string, val string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.tree.insert(key, val)
}

func remove(store *KVstore, key string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	isDeleted := store.tree.remove(key)
	return isDeleted

}

func scan(store *KVstore, startKey string, endKey string) (keys []string, values []string) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	keys, vals := store.tree.scan(startKey, endKey)
	return keys, vals

}

func applyLoop(node *RaftNode) {
	ticker := time.NewTicker(10 * time.Millisecond)
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
