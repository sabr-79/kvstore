package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

type Snapshot struct {
	LastIndex int
	LastTerm  int
	TreeData  []byte
}

func (s *Snapshot) encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeSnapshot(data []byte) (*Snapshot, error) {
	var snap Snapshot
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func saveSnapshot(nodeID string, snap *Snapshot) error {
	filename := fmt.Sprintf("raft_%s.snap", nodeID)
	data, err := snap.encode()
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

func loadSnapshot(nodeID string) (*Snapshot, error) {
	filename := fmt.Sprintf("raft_%s.snap", nodeID)
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return decodeSnapshot(data)
}

func (node *RaftNode) takeSnapshot() error {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.stateMachine.mu.Lock()
	defer node.stateMachine.mu.Unlock()

	lastApplied := node.lastApplied
	if lastApplied == 0 {
		return fmt.Errorf("no entries to snapshot")
	}
	pos := lastApplied - node.snapIndex - 1
	if pos < 0 || pos >= len(node.log) {
		return fmt.Errorf("inconsistent log index: lastApplied=%d snapIndex=%d len(log)=%d",
			lastApplied, node.snapIndex, len(node.log))
	}
	lastTerm := node.log[pos].Term

	if err := node.stateMachine.tree.pager.flush(); err != nil {
		return err
	}

	treeFile := node.stateMachine.tree.pager.file
	treeData, err := os.ReadFile(treeFile.Name())
	if err != nil {
		return err
	}

	snap := &Snapshot{
		LastIndex: lastApplied,
		LastTerm:  lastTerm,
		TreeData:  treeData,
	}

	if err := saveSnapshot(node.id, snap); err != nil {
		return err
	}

	node.log = append([]LogEntry{}, node.log[pos+1:]...)
	node.snapIndex = lastApplied
	node.snapTerm = lastTerm

	logFileName := fmt.Sprintf("raft_%s.log", node.id)
	if err := os.Remove(logFileName); err != nil && !os.IsNotExist(err) {
		return err
	}

	persist(node)
	return nil
}

func snapshotManager(node *RaftNode, threshold int) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		node.mu.Lock()
		logLen := len(node.log)
		node.mu.Unlock()
		if logLen > threshold {
			if err := node.takeSnapshot(); err != nil {
				fmt.Printf("snapshot error: %v\n", err)
			}
		}
	}
}

func restoreTree(data []byte) *TreeRoot {
	tmpFile, err := os.CreateTemp("", "snapshot_tree_*.db")
	if err != nil {
		panic(err)
	}
	if _, err := tmpFile.Write(data); err != nil {
		panic(err)
	}
	tmpFile.Close()
	return newTree(tmpFile.Name(), 4096, 128)
}
func installSnapshot(node *RaftNode, args InstallSnapshotArgs) InstallSnapshotReply {
	node.mu.Lock()
	defer node.mu.Unlock()

	if args.Term < node.currentTerm {
		return InstallSnapshotReply{Term: node.currentTerm}
	}
	if args.Term > node.currentTerm {
		node.currentTerm = args.Term
		node.votedFor = ""
		node.role = Follower
		node.leaderId = args.LeaderId
		persist(node)
	} else if node.role != Follower {
		node.role = Follower
		node.leaderId = args.LeaderId
	}

	snap, err := decodeSnapshot(args.Data)
	if err != nil {
		return InstallSnapshotReply{Term: node.currentTerm}
	}

	node.stateMachine.mu.Lock()
	newTree := restoreTree(snap.TreeData)
	node.stateMachine.tree = newTree
	node.stateMachine.mu.Unlock()

	node.lastApplied = snap.LastIndex
	node.commitIndex = snap.LastIndex
	node.snapIndex = snap.LastIndex
	node.snapTerm = snap.LastTerm
	if len(node.log) > 0 {
		cut := 0
		for cut < len(node.log) && node.log[cut].Index <= snap.LastIndex {
			cut++
		}
		node.log = append([]LogEntry{}, node.log[cut:]...)
	}
	persist(node)

	return InstallSnapshotReply{Term: node.currentTerm}
}
