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
// rf[State() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

const RandomFactor float64 = 0.8
const HeartbeatInterval = 100

func getRandomElectionTimeout() time.Duration {
	const baseTime int64 = 1000
	randomTime := int64(float64(rand.Int63()%baseTime) * RandomFactor)
	return time.Duration(randomTime+baseTime) * time.Millisecond
}

func getRandomHeartbeatTimeout() time.Duration {
	const baseTime int64 = 300
	randomTime := int64(float64(rand.Int63()%baseTime) * RandomFactor)
	return time.Duration(randomTime+baseTime) * time.Millisecond
}

func (rf *Raft) setElectionTimeout(timeout time.Duration) {
	rf.electionTime = time.Now().Add(timeout)
}

func (rf *Raft) setHeartbeatTimeout(timeout time.Duration) {
	rf.heartbeatTime = time.Now().Add(timeout)
}

type Role int

const (
	Follower  Role = 1
	Candidate Role = 2
	Leader    Role = 3
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

	leaderId int
	role     Role
	//electionTimeoutDuration  time.Duration
	//heartbeatTimeoutDuration time.Duration

	electionTime  time.Time
	heartbeatTime time.Time
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil {
		BetterDebug(dError, "Error encoding rf.currentTerm. %v\n", rf.currentTerm)
	}
	if e.Encode(rf.votedFor) != nil {
		BetterDebug(dError, "Error encoding rf.votedFor. %v\n", rf.votedFor)
	}
	if e.Encode(rf.log) != nil {
		BetterDebug(dError, "Error encoding rf.log. %v\n", rf.log)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	log := rf.log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		BetterDebug(dError, "Error decoding\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
		rf.role = Follower
		rf.votedFor = -1
		rf.currentTerm = termToCheck
		rf.leaderId = -1
		rf.persist()
		return true
	}
	return false
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

	term = rf.currentTerm
	entry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, entry)
	// index needs to be computed AFTER appending new log entry
	index = len(rf.log)
	rf.persist()
	rf.sendEntriesToPeers(false)
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

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[server] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) maybeUpdateCommitIndex() {
	for N := rf.log.lastIndex(); N > rf.commitIndex && rf.log.get(N).Term == rf.currentTerm; N-- {
		count := 1
		for server, matchIndex := range rf.matchIndex {
			if server != rf.me && matchIndex >= N {
				count++
			}
		}
		majority := len(rf.peers) / 2
		if count > majority {
			BetterDebug(dCommit, "Server %v updated commitIndex at term %v for majority consensus. CI: %v",
				rf.me, rf.currentTerm, rf.commitIndex)
			rf.commitIndex = N
			break
		}
	}
}

func (rf *Raft) becomeLeader() {
	BetterDebug(dLeader, "Server %v received the majority votes. It is now leader.\n", rf.me)
	rf.changeRoleTo(Leader)
	lastLogIndex := rf.log.lastIndex()
	for server := range rf.peers {
		rf.nextIndex[server] = lastLogIndex + 1
		rf.matchIndex[server] = 0
	}
	// send initial heartbeat
	rf.sendEntriesToPeers(true)
}

func (rf *Raft) handleCandidateRequestingVote(args *RequestVoteArgs, server int, numVotes *int, once *sync.Once) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		BetterDebug(dError, "RequestVote RPC failed to send from server %v to peer %v\n", rf.me, server)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		BetterDebug(dInfo, "RequestVote RPC successfully sent from server %v to peer %v\n", rf.me, server)
		if reply.Term < rf.currentTerm || rf.currentTerm != args.Term {
			// invalid reply
			return
		}
		rf.checkOrUpdateTerm(reply.Term)
		if reply.VoteGranted {
			*numVotes++ // need to verify this doesn't cause a race
			BetterDebug(dVote, "Server %v got a vote by server %v on term %v\n", rf.me, server, rf.currentTerm)
			majority := len(rf.peers) / 2
			if *numVotes > majority {
				once.Do(rf.becomeLeader)
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
	rf.currentTerm++
	BetterDebug(dTerm, "Server %v has started a new term. Election has started. %v\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	rf.setElectionTimeout(getRandomElectionTimeout())
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  rf.log.lastTerm(),
	}
	once := sync.Once{}
	numVotes := 1 // start with one because server voted for itself
	for server := range rf.peers {
		// skip current server
		if server != rf.me {
			BetterDebug(dVote, "Server %v is sending RequestVote to peer %v\n", rf.me, server)
			go rf.handleCandidateRequestingVote(args, server, &numVotes, &once)
		}
	}

}

func (rf *Raft) handleLeaderSendingEntries(args *AppendEntriesArgs, server int) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		// print something for debugging
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term < rf.currentTerm || rf.currentTerm != args.Term || rf.checkOrUpdateTerm(reply.Term) {
			// invalid reply
			return
		}
		if reply.Success {
			possibleNewNext := args.PrevLogIndex + 1 + len(args.Entries)
			possibleNewMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = max(possibleNewNext, rf.nextIndex[server])
			rf.matchIndex[server] = max(possibleNewMatch, rf.matchIndex[server])
			rf.maybeUpdateCommitIndex()
		} else {
			// call not successful, retry the request

			// optimization
			if reply.ConflictingTerm == -1 {
				rf.nextIndex[server] = reply.LogLength + 1
			} else {
				_, end := rf.log.findTermRange(reply.ConflictingTerm)
				if end != -1 {
					rf.nextIndex[server] = end
				} else {
					rf.nextIndex[server] = reply.ConflictingIndex
				}
			}
			last := rf.log.lastIndex()
			next := rf.nextIndex[server]
			if last >= next {
				entries := make([]LogEntry, last-next+1)
				copy(entries, rf.log.slice(next, last+1))
				retryArgs := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: next - 1,
					PrevLogTerm:  rf.log.get(next - 1).Term,
					Entries:      entries,
				}
				go rf.handleLeaderSendingEntries(retryArgs, server)
			}
		}
	}
}

func (rf *Raft) sendEntriesToPeers(heartbeatOnly bool) {
	BetterDebug(dLeader, "Server %v is sending entries to peers. Log looks like %v\n", rf.me, rf.log)
	rf.setHeartbeatTimeout(time.Duration(HeartbeatInterval) * time.Millisecond)
	rf.setElectionTimeout(getRandomHeartbeatTimeout())
	last := rf.log.lastIndex()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		next := rf.nextIndex[server]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.log.get(next - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if last >= next {
			entries := make([]LogEntry, last-next+1)
			copy(entries, rf.log.slice(next, last+1))
			args.Entries = entries
			go rf.handleLeaderSendingEntries(args, server)
		} else if heartbeatOnly {
			args.Entries = make([]LogEntry, 0)
			go rf.handleLeaderSendingEntries(args, server)
		}
	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		switch rf.role {
		case Leader:
			if time.Now().After(rf.heartbeatTime) {
				BetterDebug(dLeader, "Server %v (leader) sending heartbeat at term %v.\n", rf.me, rf.currentTerm)
				rf.sendEntriesToPeers(true)
			}
		case Candidate:
			if time.Now().After(rf.electionTime) {
				BetterDebug(dCandidate, "Server %v (candidate) starting election at term %v.\n", rf.me, rf.currentTerm)
				rf.startElection()
			}
		case Follower:
			if time.Now().After(rf.electionTime) {
				BetterDebug(dInfo, "Server %v (follower) turning into candidate and starting election\n", rf.me, rf.currentTerm)
				rf.changeRoleTo(Candidate)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(30) * time.Millisecond)
	}
}

func (rf *Raft) sendLogsThroughApplyCh(applyCh chan ApplyMsg) {
	// add messages until we get to commit index
	for rf.killed() == false {
		rf.mu.Lock()
		messages := []ApplyMsg{}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				Command:      rf.log.get(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			})
			BetterDebug(dLog, "Server %v is applying log at term %v, last applied is %v, commitIndex is %v, log %v\n",
				rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.log)
		}
		rf.mu.Unlock()
		for _, message := range messages {
			applyCh <- message
		}
		time.Sleep(time.Duration(30) * time.Millisecond)
	}

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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.role = Follower

	rf.setElectionTimeout(getRandomHeartbeatTimeout())

	rf.log = make(Entries, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	BetterDebug(dInfo, "Initializing server %v...\n", me)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to send logs through applyCh
	go rf.sendLogsThroughApplyCh(applyCh)

	return rf
}
