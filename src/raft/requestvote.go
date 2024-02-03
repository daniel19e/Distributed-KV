package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// This RPC does the following:
	//1. Reply false if term < currentTerm (§5.1)
	//2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	BetterDebug(dVote, "Server %v got a vote request at term %v\n", rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// update current term if term of candidate is higher
	rf.checkOrUpdateTerm(args.Term)

	// check candidate's log is at least as up-to-date as receiver's log
	lastLogIndex := rf.log.getLastIndex()
	lastLogTerm := rf.log.peak().Term
	isLogUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if isLogUpToDate {
			BetterDebug(dVote, "Server %v log is up to date. Vote granted\n", rf.me)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId

			rf.setElectionTimeout(randElectionTimeout())

		} else {
			BetterDebug(dVote, "Server %v log is not up to date. Vote rejected.\n", rf.me)
		}
	} else {
		// already voted
	}
}
