package main

import (
	"encoding/gob"
	"fmt"
	"os"
)

type RaftSnapshot struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
}

func persist(node *RaftNode) {
	filename := fmt.Sprintf("raft_%s.state", node.id)
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("error creating file")
		return
	}
	encode := gob.NewEncoder(file)
	var current = RaftSnapshot{CurrentTerm: node.currentTerm, VotedFor: node.votedFor, Log: node.log}
	err = encode.Encode(&current)
	if err != nil {
		fmt.Println("error encoding state")
	}
	file.Sync()
	file.Close()

}

func readPersistFile(node *RaftNode) {

	filename := fmt.Sprintf("raft_%s.state", node.id)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("no such file")
		return
	}

	decode := gob.NewDecoder(file)
	var recovered RaftSnapshot
	err = decode.Decode(&recovered)
	if err != nil {
		fmt.Println("error decoding file")
	}
	node.votedFor = recovered.VotedFor
	node.currentTerm = recovered.CurrentTerm
	node.log = recovered.Log

	file.Close()

}
