package main

import (
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

type LogEntry struct {
	Index   int
	Term    int
	Command string
}

type RaftNode struct {
	mu              sync.Mutex
	currentTerm     int
	votedFor        string
	votesReceived   int
	log             []LogEntry
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
	leaderId        string
	isDead          bool
	pendingLog      []LogEntry
	batchsize       int
}

func newRaftNode(id string, peers []string) *RaftNode {
	newNode := RaftNode{role: Follower, currentTerm: 0, votedFor: "", id: id, peers: peers, electionResetCh: make(chan struct{}, 1), batchsize: 100}
	readPersistFile(&newNode)
	go electionTimer(&newNode)
	go applyLoop(&newNode)
	return &newNode

}

func electionTimer(node *RaftNode) {
	for {
		randomDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
		select {
		case <-time.After(randomDuration):
			node.mu.Lock()
			isLeader := node.role
			if node.isDead {
				node.mu.Unlock()
				continue
			}
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
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		//persistLog(node)
		node.mu.Lock()
		if node.isDead {
			node.mu.Unlock()
			continue
		}
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
	persist(node)

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
		persist(node)
	}
	if node.votedFor == "" || node.votedFor == arg.CandidateId {
		voteGranted = true
		node.votedFor = arg.CandidateId
		persist(node)
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
			persist(node)

		} else if arg.Term == node.currentTerm && node.role != Follower {
			node.role = Follower
			node.leaderId = arg.LeaderId
		}
		for _, entry := range arg.Entries {
			matchEntry := entry.Index - 1

			if matchEntry < len(node.log) {
				if entry.Term != node.log[matchEntry].Term || node.log[matchEntry].Command != entry.Command {
					node.log = node.log[:matchEntry]
					if len(node.pendingLog) >= node.batchsize {
						node.mu.Unlock()
						persistLog(node)
						node.mu.Lock()
					}
				}

			}

			if len(node.log) == matchEntry {
				node.log = append(node.log, entry)
				persist(node)
			}

		}
		if arg.LeaderCommit > node.commitIndex {
			node.commitIndex = min(arg.LeaderCommit, len(node.log))
		}

	}
	return ReplyEntriesArgs{Term: node.currentTerm, Success: true}
}
