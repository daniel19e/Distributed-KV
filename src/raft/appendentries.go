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
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.NextTryIndex = rf.log.lastIndex() + 1
		return // reply false if term < currentTerm
	}

	rf.checkOrUpdateTerm(args.Term)

	if args.PrevLogIndex > rf.log.lastIndex() {
		// reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.NextTryIndex = rf.log.lastIndex() + 1
		return
	}
	rf.setElectionTimeout(getRandomHeartbeatTimeout())
	//rf.setHeartbeatTimeout(getRandomHeartbeatTimeout())
	rf.leaderId = args.LeaderId

	if args.PrevLogIndex >= 0 && rf.log.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		term := rf.log.get(args.PrevLogIndex).Term
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log.get(i).Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= -1 {
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex())
			BetterDebug(dCommit, "Server %v has updated commitIndex to %v at term %v.\n", rf.me, rf.commitIndex, rf.currentTerm)
			go rf.sendLogsThroughApplyCh()
		}
	}
	reply.Success = true
}
