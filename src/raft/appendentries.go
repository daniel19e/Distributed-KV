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
	Term    int
	Success bool
	// these 3 fields are for implementing an optimization needed to solve
	// the issue described in Figure 8 of the Raft paper
	// the optimization tries to reduce the number of rejected AppendEntries RPCs
	// by decrementing nextIndex to bypass all of the conflicting entries in a given term
	ConflictingTerm  int
	ConflictingIndex int
	LogLength        int
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
	rf.resetElectionTime(getRandomHeartbeatTimeout())
	rf.leaderId = args.LeaderId

	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.getLogEntryAt(args.PrevLogIndex).Term {
		// reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm

		// optimization
		reply.LogLength = len(rf.log)
		reply.ConflictingTerm = rf.getLogEntryAt(args.PrevLogIndex).Term
		reply.ConflictingIndex, _ = rf.findTermRange(reply.ConflictingTerm)
		return
	}
	rf.leaderId = args.LeaderId

	for i, entry := range args.Entries {
		if rf.getLogEntryAt(i+1+args.PrevLogIndex).Term != entry.Term {
			rf.log = append(rf.getTrimmedLogSlice(1+rf.lastIncludedIndex, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}

	if len(args.Entries) > 0 {
		// store current raft state if new entries came in
		rf.persist( /*snapshot = */ false)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}
	reply.Success = true
}
