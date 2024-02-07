package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      Entries
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
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
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return // reply false if term < currentTerm
	}
	if rf.role == Candidate && rf.currentTerm == args.Term {
		rf.role = Follower
	}

	rf.checkOrUpdateTerm(args.Term)
	reply.Term = rf.currentTerm
	rf.setElectionTimeout(getRandomHeartbeatTimeout())
	rf.leaderId = args.LeaderId

	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.log.get(args.PrevLogIndex).Term {
		// reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		return
	}
	rf.leaderId = args.LeaderId

	for i, entry := range args.Entries {
		if rf.log.get(i+1+args.PrevLogIndex).Term != entry.Term {
			rf.log = append(rf.log.slice(1, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}
	reply.Success = true
}
