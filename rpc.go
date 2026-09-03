package main

import (
	"bytes"
	"encoding/json"
	"net/http"
)

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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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

func sendInstallSnapshot(peerAddr string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	body, _ := json.Marshal(args)
	resp, err := http.Post("http://"+peerAddr+"/install-snapshot", "application/json", bytes.NewReader(body))
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	defer resp.Body.Close()
	var reply InstallSnapshotReply
	err = json.NewDecoder(resp.Body).Decode(&reply)
	return reply, err
}
