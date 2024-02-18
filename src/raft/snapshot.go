package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	BetterDebug(dSnap, "InstallSnapshot RPC being called in term %v\n", rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.checkOrUpdateTerm(args.Term)
	reply.Term = rf.currentTerm
	rf.resetElectionTime(getRandomHeartbeatTimeout())

	if rf.tempLastIncludedIndex >= args.LastIncludedIndex {
		BetterDebug(dSnap, "Server %v has a newer snapshot from leader %v\n", rf.me, args.LeaderId)
		return
	}
	if rf.lastIndex() >= args.LastIncludedIndex {
		rf.log = rf.getTrimmedLogSlice(args.LastIncludedIndex+1, rf.lastIndex()+1)
	} else {
		rf.log = Entries{}
	}

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	//rf.commitIndex = args.LastIncludedIndex
	//rf.lastApplied = args.LastIncludedIndex

	rf.leaderId = args.LeaderId
	rf.tempLastIncludedIndex = args.LastIncludedIndex
	rf.tempLastIncludedTerm = args.LastIncludedTerm
	rf.tempSnapshot = args.Data

	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()
}
