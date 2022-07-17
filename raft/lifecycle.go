package raft

import (
	"math"
	"math/rand"
	"time"
)

const MinElectionTimeout = 500
const MaxElectionTimeout = 1000

//获取随机的超时时间区间
func randTimeout() time.Duration {
	randTimeout := MinElectionTimeout + rand.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(randTimeout) * time.Millisecond
}

func (rf *Raft) manageFollower() {
	duration := randTimeout()
	time.Sleep(duration)
	rf.mu.Lock()
	lastAccessed := rf.lastAccessed
	rf.mu.Unlock()
	if time.Now().Sub(lastAccessed).Milliseconds() >= duration.Milliseconds() {
		rf.mu.Lock()
		rf.status = Candidate
		rf.currentTerm++
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) manageCandidate() {
	timeOut := randTimeout()
	start := time.Now()
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndex
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()
	count := 0
	total := len(peers)
	finished := 0
	majority := (total / 2) + 1

	for peer := range peers {
		if me == peer {
			rf.mu.Lock()
			count++
			finished++
			rf.mu.Unlock()
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = me
			args.LastLogTerm = lastLogTerm
			args.LastLogIndex = lastLogIndex

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok {
				finished++
				return
			}

			if reply.VoteGranted {
				finished++
				count++
			} else {
				finished++
				if args.Term < reply.Term {
					rf.status = Follower
					rf.persist()
				}
			}

		}(peer)
	}

	for {
		rf.mu.Lock()
		if count >= majority || finished == total || time.Now().Sub(start).Milliseconds() >= timeOut.Milliseconds() {
			break
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	if time.Now().Sub(start).Milliseconds() >= timeOut.Milliseconds() {
		rf.status = Follower
		rf.mu.Unlock()
		return
	}

	if rf.status == Candidate && count >= majority {
		rf.status = Leader
		for peer := range peers {
			rf.nextIndex[peer] = rf.lastLogIndex + 1
		}
	} else {
		rf.status = Follower
	}

	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) manageLeader() {
	rf.mu.Lock()
	me := rf.me
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	peers := rf.peers
	nextIndex := rf.nextIndex

	lastLogIndex := rf.lastLogIndex
	matchIndex := rf.matchIndex
	nextIndex[me] = lastLogIndex + 1
	matchIndex[me] = lastLogIndex
	log := rf.log

	rf.mu.Unlock()

	for n := commitIndex + 1; n < lastLogIndex; n++ {
		count := 0
		total := len(peers)
		majority := (total / 2) + 1

		for peer := range peers {
			if matchIndex[peer] >= n && log[n].Term == term {
				count++
			}
		}

		if count >= majority {
			rf.mu.Lock()
			i := rf.commitIndex + 1
			for ; i < n; i++ {
				rf.applyMsg <- ApplyMsg{
					CommandValid: true,
					Command:      log[i].Command,
					CommandIndex: i,
				}
				rf.commitIndex = rf.commitIndex + 1
			}
			rf.mu.Lock()
		}
	}

	for peer := range peers {
		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}

		rf.mu.Lock()

		args.Term = rf.currentTerm
		prevLogIndex := nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.log[prevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		args.LeaderId = rf.me

		if nextIndex[peer] <= lastLogIndex {
			args.Entries = rf.log[prevLogIndex+1 : lastLogIndex+1]
		}
		rf.mu.Unlock()

		go func(peer int) {
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Success {
				rf.nextIndex[peer] = int(math.Min(float64(rf.nextIndex[peer]+len(args.Entries)), float64(rf.lastLogIndex+1)))
				rf.matchIndex[peer] = prevLogIndex + len(args.Entries)
			} else {
				if reply.Term > args.Term {
					rf.status = Follower
					return
				}

				if reply.XTerm == -1 {
					rf.nextIndex[peer] = reply.XLen
					return
				}

				index := -1
				for i, v := range rf.log {
					if v.Term == reply.XTerm {
						index = i
					}
				}

				if index == -1 {
					rf.nextIndex[peer] = reply.XIndex
				} else {
					rf.nextIndex[peer] = index
				}
			}

		}(peer)

	}
}
