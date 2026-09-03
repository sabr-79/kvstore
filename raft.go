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
	snapIndex       int
	snapTerm        int
	snapsize        int
}

func newRaftNode(id string, peers []string) *RaftNode {
	newNode := RaftNode{role: Follower, currentTerm: 0, votedFor: "", id: id, peers: peers, electionResetCh: make(chan struct{}, 1), batchsize: 100, snapsize: 100}
	if snap, err := loadSnapshot(id); err == nil && snap != nil {
		newNode.stateMachine = &KVstore{tree: restoreTree(snap.TreeData)}
		newNode.snapIndex = snap.LastIndex
		newNode.snapTerm = snap.LastTerm
		readPersistFile(&newNode)
	} else {
		readPersistFile(&newNode)
	}
	go electionTimer(&newNode)
	go applyLoop(&newNode)
	if newNode.snapsize > 0 {
		go snapshotManager(&newNode, newNode.snapsize)
	}
	return &newNode

}

func electionTimer(node *RaftNode) {
	for {
		randomDuration := time.Duration(500+rand.Intn(500)) * time.Millisecond
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
	// in the future, make this adaptive.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		persistLog(node)
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
			snapIdx := node.snapIndex
			prevIdx := nxt - 1
			if prevIdx < snapIdx {
				prevIdx = snapIdx
			}
			var prevTerm int
			if prevIdx == 0 {
				prevTerm = 0
			} else if prevIdx <= snapIdx {
				prevTerm = node.snapTerm
			} else {
				logPos := prevIdx - snapIdx - 1
				if logPos >= 0 && logPos < len(node.log) {
					prevTerm = node.log[logPos].Term
				} else {
					prevTerm = 0
				}
			}
			var entriesToSend []LogEntry
			start := prevIdx - snapIdx
			if start >= 0 && start < len(node.log) {
				entriesToSend = node.log[start:]
			} else {
				entriesToSend = nil
			}
			newArg := AppendEntriesArgs{Term: term, LeaderId: id, PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, Entries: entriesToSend, LeaderCommit: node.commitIndex}
			node.mu.Unlock()

			if snapIdx > 0 && nxt <= snapIdx {
				go func(p string) {
					snap, err := loadSnapshot(node.id)
					if err != nil || snap == nil {
						return
					}
					data, err := snap.encode()
					if err != nil {
						return
					}
					snapArgs := InstallSnapshotArgs{
						Term:              term,
						LeaderId:          id,
						LastIncludedIndex: snap.LastIndex,
						LastIncludedTerm:  snap.LastTerm,
						Data:              data,
					}
					reply, err := sendInstallSnapshot(p, snapArgs)
					if err != nil {
						return
					}
					node.mu.Lock()
					defer node.mu.Unlock()
					if reply.Term > node.currentTerm {
						node.role = Follower
						node.currentTerm = reply.Term
						node.votedFor = ""
					} else if reply.Term == term {
						node.nextIndex[p] = snap.LastIndex + 1
						node.matchIndex[p] = snap.LastIndex
					}
				}(peer)
				continue
			}

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
					node.matchIndex[p] = args.PrevLogIndex + len(args.Entries)
					node.nextIndex[p] = node.matchIndex[p] + 1

					for i := node.commitIndex + 1; i <= len(node.log)+node.snapIndex; i++ {
						var entryTerm int
						if i <= node.snapIndex {
							entryTerm = node.snapTerm
						} else {
							logPos := i - node.snapIndex - 1
							if logPos >= 0 && logPos < len(node.log) {
								entryTerm = node.log[logPos].Term
							}
						}
						count := 1
						for _, match := range node.matchIndex {
							if match >= i {
								count++
							}

						}

						if count >= (len(node.peers)+1)/2+1 {
							if entryTerm == node.currentTerm {
								node.commitIndex = i
							}
						}

					}

				}
				if !reply.Success {
					if node.nextIndex[p] > 1 {
						node.nextIndex[p]--

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

	lastIdx := node.snapIndex + len(node.log)
	lastTerm := 0
	if lastIdx > 0 {
		if lastIdx == node.snapIndex {
			lastTerm = node.snapTerm
		} else {
			pos := lastIdx - node.snapIndex - 1
			if pos >= 0 && pos < len(node.log) {
				lastTerm = node.log[pos].Term
			}
		}
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
		node.nextIndex[peer] = node.snapIndex + len(node.log) + 1
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
		if node.snapIndex+len(node.log) < arg.PrevLogIndex {
			return ReplyEntriesArgs{Term: node.currentTerm, Success: false}
		}
		if arg.PrevLogIndex > node.snapIndex {
			pos := arg.PrevLogIndex - node.snapIndex - 1
			if pos < 0 || pos >= len(node.log) {
				return ReplyEntriesArgs{Term: node.currentTerm, Success: false}
			}
			if node.log[pos].Term != arg.PrevLogTerm {
				return ReplyEntriesArgs{Term: node.currentTerm, Success: false}
			}
		} else if arg.PrevLogIndex == node.snapIndex {
			if node.snapTerm != arg.PrevLogTerm {
				return ReplyEntriesArgs{Term: node.currentTerm, Success: false}
			}
		} else {
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
			pos := entry.Index - node.snapIndex - 1
			if pos < 0 {
				continue
			}
			if pos < len(node.log) {
				if entry.Term != node.log[pos].Term || node.log[pos].Command != entry.Command {
					node.log = node.log[:pos]
					node.pendingLog = nil
					persist(node)
				}
			}
			if len(node.log) == pos {
				node.log = append(node.log, entry)
				node.pendingLog = append(node.pendingLog, entry)
				if len(node.pendingLog) >= node.batchsize {
					node.mu.Unlock()
					persistLog(node)
					node.mu.Lock()
				}

			}

		}
		if arg.LeaderCommit > node.commitIndex {
			node.commitIndex = min(arg.LeaderCommit, node.snapIndex+len(node.log))
		}

	}
	return ReplyEntriesArgs{Term: node.currentTerm, Success: true}
}
