package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      Entries
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// This RPC does the following:
	//1. Reply false if term < currentTerm (§5.1)
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	//3. If an existing entry conflicts with a new one (same index but different terms),
	//delete the existing entry and all that follow it (§5.3)
	//4. Append any new entries not already in the log
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	BetterDebug(dInfo, "AppendEntries RPC being called in term %v\n", rf.currentTerm)
	reply.Term = rf.currentTerm
	rf.lastHeartbeatTime = time.Now()
	reply.Success = false
	if args.Term < rf.currentTerm {
		return // reply false if term < currentTerm
	}
	if args.Entries.getEntryAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		// reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		return
	}

	if rf.role == Candidate && rf.currentTerm == args.Term {
		rf.role = Follower
	}
	// conflicting entry
	rf.checkOrUpdateTerm(args.Term)
	rf.setElectionTimeout(randHeartbeatTimeout())
	//rf.electionTimeoutDuration = randomizedElectionTimeout()
	rf.leaderId = args.LeaderId

	for i, entry := range args.Entries {
		indexToGet := i + 1 + args.PrevLogIndex
		if rf.log.getEntryAt(indexToGet).Term != entry.Term {
			rf.log = append(rf.log.slice(1, indexToGet), args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// set commit index
		lastNewEntry := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntry)
		BetterDebug(dCommit, "Server %v has updated commitIndex to %v at term %v.\n", rf.me, rf.commitIndex, rf.currentTerm)
	}
	reply.Success = true
}
