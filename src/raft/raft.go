package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const HeartbeatTime = 100

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	leaderId                 int
	role                     Role
	lastHeartbeatTime        time.Time
	electionTimeoutDuration  time.Duration
	heartbeatTimeoutDuration time.Duration

	// persistent state
	currentTerm int
	votedFor    int
	log         Entries

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state (reinitiated after election)
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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

	// conflicting entry
	rf.checkOrUpdateTerm(args.Term)
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
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) maybeUpdateCommitIndex() {
	for N := len(rf.log); N > rf.commitIndex && rf.log.getEntryAt(N).Term == rf.currentTerm; N-- {
		count := 1
		for server, matchIndex := range rf.matchIndex {
			if server != rf.me {
				if matchIndex >= N {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			BetterDebug(dCommit, "Server %v updated commitIndex at term %v for majority consensus. CI: %v",
				rf.me, rf.currentTerm, rf.commitIndex)
			break
		}
	}
}
func (rf *Raft) handleSendingAppendEntries(s int, args AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(s, &args, &reply)
	if !ok {
		BetterDebug(dError, "AppendEntries RPC failed to send from server %v to peer %v\n", rf.me, s)
	} else {
		if reply.Term < rf.currentTerm {
			return
		}
		if rf.checkOrUpdateTerm(reply.Term) {
			return
		}
		if reply.Success {
			BetterDebug(dLog, "Server %v gets Server %v Log entries at term %v\n", rf.me, s, rf.currentTerm)
			updatedNext := args.PrevLogIndex + 1 + len(args.Entries)
			updatedMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[s] = max(updatedNext, rf.nextIndex[s])
			rf.matchIndex[s] = max(updatedMatch, rf.matchIndex[s])
			//	rf.maybeUpdateCommitIndex()
		} else {
			if rf.nextIndex[s] > 1 {
				rf.nextIndex[s]--
			}
			last := len(rf.log)
			next := rf.nextIndex[s]
			if last >= next {
				// retry
				BetterDebug(dLog, "Server %v <- Server %v logs are not consistent, retrying.", rf.me, s)
				entries := make([]LogEntry, last-next+1)
				copy(entries, rf.log.slice(next, last+1))
				newArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: next - 1,
					PrevLogTerm:  rf.log.getEntryAt(next - 1).Term,
					Entries:      entries,
				}
				go rf.handleSendingAppendEntries(s, newArgs)
			}
		}
	}
}
func (rf *Raft) sendEntriesToPeers() {
	BetterDebug(dLeader, "Server %v is sending entries to peers. Log looks like %v\n", rf.me, rf.log)
	rf.electionTimeoutDuration = randomizedElectionTimeout()
	lastLogIdx := len(rf.log)
	for server := range rf.peers {
		if server != rf.me {
			next := rf.nextIndex[server]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: next - 1,
				PrevLogTerm:  rf.log.getEntryAt(next - 1).Term,
				LeaderCommit: rf.commitIndex}
			if lastLogIdx >= next {
				newEntries := make([]LogEntry, lastLogIdx-next+1)
				copy(newEntries, rf.log.slice(next, lastLogIdx+1))
				args.Entries = newEntries
				go rf.handleSendingAppendEntries(server, args)
			}
		}
	}
}

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

func (rf *Raft) changeRoleTo(role Role) {
	BetterDebug(dInfo, "Server %v trying to switch role %v to role %v\n", rf.me, rf.role, role)
	if rf.role != role {
		BetterDebug(dInfo, "Server %v switched roles\n", rf.me)
		rf.role = role
	}
	if role == Follower {
		rf.votedFor = -1 // reset voted for
	}
}

// If RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) checkOrUpdateTerm(termToCheck int) bool {
	if rf.currentTerm < termToCheck {
		BetterDebug(dInfo, "RPC request or response contains term T > currentTerm. T = %v, currentTerm = %v\n", termToCheck, rf.currentTerm)
		rf.currentTerm = termToCheck
		rf.changeRoleTo(Follower)
		return true
	}
	return false
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
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	isLogUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if isLogUpToDate {
			BetterDebug(dVote, "Server %v log is up to date. Vote granted\n", rf.me)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			BetterDebug(dVote, "Server %v log is not up to date. Vote rejected.\n", rf.me)
		}
	} else {
		// already voted
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	entry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, entry)
	index = len(rf.log)
	term = rf.currentTerm

	// call appendEntries RPC
	rf.sendEntriesToPeers()
	BetterDebug(dLog, "Server %v added command %v (index %v) to the log in term %v\n", rf.me, command, index, term)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeat() {
	// this method sends an empty AppendEntries RPC to all the peers
	// to notify them that the current server is the leader
	BetterDebug(dInfo, "Sending heartbeat as leader %v at term %v\n", rf.me, rf.currentTerm)

	// prepare AppendEntriesArgs with empty Entries for heartbeat
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	for server := range rf.peers {
		if server != rf.me { // skip self
			go func(server int) { // send RPCs in parallel
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					BetterDebug(dError, "AppendEntries RPC failed to send from server %v to peer %v\n", rf.me, server)
				} else {
					if reply.Term < rf.currentTerm {
						return
					}
					if rf.currentTerm != args.Term {
						return
					}
					if rf.checkOrUpdateTerm(reply.Term) {
						return
					}
				}
			}(server)
		}
	}
}

func (rf *Raft) handleCandidateRequestingVote(args RequestVoteArgs, server int, self int, numVotes *int, once *sync.Once) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		BetterDebug(dError, "RequestVote RPC failed to send from server %v to peer %v\n", self, server)
	} else {
		BetterDebug(dInfo, "RequestVote RPC successfully sent from server %v to peer %v\n", self, server)

		// if AppendEntries RPC received from new leader: convert to follower
		rf.checkOrUpdateTerm(reply.Term)

		if reply.VoteGranted {
			*numVotes++ // need to verify this doesn't cause a race
			BetterDebug(dVote, "Server %v got a vote by server %v on term %v\n", self, server, rf.currentTerm)
			majority := len(rf.peers) / 2
			if *numVotes > majority {
				once.Do(
					func() {
						BetterDebug(dLeader, "Server %v received the majority votes. It is now leader.\n", self)
						rf.role = Leader
						lastLogIndex := len(rf.log) - 1
						for peer := range rf.peers {
							rf.nextIndex[peer] = lastLogIndex + 1
							rf.matchIndex[peer] = 0
						}
						// send initial heartbeat
						rf.sendHeartbeat()
					})
			}
		}

	}
}

func (rf *Raft) startElection() {
	// This method does the following:
	// 1. increment currentTerm
	// 2. vote for self
	// 3. reset election timer
	// 4. send RequestVote RPCs to all other servers
	rf.changeRoleTo(Candidate)
	rf.currentTerm++
	BetterDebug(dTerm, "Server %v has started a new term. Election has started. %v\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.electionTimeoutDuration = randomizedElectionTimeout()
	numVotes := 1 // start with one because server voted for itself
	for server := range rf.peers {
		// skip current server
		if server != rf.me {
			BetterDebug(dVote, "Server %v is sending RequestVote to peer %v\n", rf.me, server)
			go func(s int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: 0,
					LastLogTerm:  0,
				}
				self := rf.me
				rf.mu.Unlock()
				once := sync.Once{}
				rf.handleCandidateRequestingVote(args, s, self, &numVotes, &once)
			}(server)
		}
	}

}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Check if a leader election should be started.
		rf.mu.Lock()
		elapsed := time.Since(rf.lastHeartbeatTime)
		electionTimeoutDuration := rf.electionTimeoutDuration
		heartbeatDuration := rf.heartbeatTimeoutDuration
		role := rf.role
		self := rf.me
		curTerm := rf.currentTerm
		rf.mu.Unlock()

		switch role {
		case Leader:
			if elapsed > heartbeatDuration {
				BetterDebug(dLeader, "Server %v (leader) sending heartbeat at term %v. Time elapsed is %v, heartbeat duration is %v.\n", self, curTerm, elapsed, heartbeatDuration)
				go rf.sendHeartbeat()
			}
		case Candidate:
			if elapsed > electionTimeoutDuration {
				BetterDebug(dCandidate, "Server %v (candidate) starting election at term %v. Time elapsed is %v, election timeout is %v.\n", self, curTerm, elapsed, electionTimeoutDuration)
				go rf.startElection()
			}
		case Follower:
			// If election timeout elapses without receiving AppendEntries RPC
			// from current leader or granting vote to candidate: convert to candidate
			if elapsed > electionTimeoutDuration {
				BetterDebug(dInfo, "Server %v (follower) turning into candidate and starting election\n", self, curTerm)
				rf.changeRoleTo(Candidate)
				go rf.startElection()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 30
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendLogsThroughApplyCh(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		messages := []ApplyMsg{}
		// add messages until we get to commit index
		for rf.lastApplied < rf.commitIndex {
			BetterDebug(dLog, "Server %v is applying log at term %v, last applied is %v, commitIndex is %v\n",
				rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			rf.lastApplied++
			messages = append(messages,
				ApplyMsg{
					CommandValid: true,
					Command:      rf.log.getEntryAt(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
				})
		}
		rf.mu.Unlock()
		// send every message through the channel
		for _, message := range messages {
			applyCh <- message
		}
		ms := 30
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func randomizedElectionTimeout() time.Duration {
	extraTime := int64(float64(rand.Int63() % 400))
	return time.Duration(extraTime+400) * time.Millisecond
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.votedFor = -1
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.role = Follower
	rf.lastHeartbeatTime = time.Now()
	rf.heartbeatTimeoutDuration = time.Duration(100) * time.Millisecond
	rf.electionTimeoutDuration = randomizedElectionTimeout()
	rf.log = make(Entries, 1)
	rf.nextIndex = make([]int, len(peers)) // init to leader last log index + 1
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	BetterDebug(dInfo, "Initializing server %v...\n", me)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to send logs through applyCh
	go rf.sendLogsThroughApplyCh(applyCh)

	return rf
}
