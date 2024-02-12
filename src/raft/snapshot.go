package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return
	}
	// create new snapshot

	// write data into snapshot file at given offset
	if !args.Done {
		// reply and wait for more data chunks
	}
	// save snapshot file, discard any existing or partial snapshot with smaller index

	// if existing log entry has same index and term as snapshot's last included entry,
	// retain log entries following it and reply

	// discard the entire log

	// reset state machine using snapshot contents (and load snapshot cluster configuration)
}
