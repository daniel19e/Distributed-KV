package shardctrler

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

var Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs           []Config // indexed by config num
	duplicateMap      map[int64]int64
	waitMap           map[int]chan Op
	lastSnapshotIndex int
}

type Op struct {
	// Your data here.
	Type        string
	ClientId    int64
	ReqId       int64
	QueryNum    int
	JoinServers map[int][]string
	LeaveGids   []int
	MoveShard   int
	MoveGid     int
}

func (sc *ShardCtrler) getOrCreateOpChannel(index int, atomic bool) chan Op {
	if atomic {
		sc.mu.Lock()
		defer sc.mu.Unlock()
	}
	ch, exists := sc.waitMap[index]
	if !exists {
		ch = make(chan Op, 1)
		sc.waitMap[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	DPrintf("server is calling join with join servers %v\n", args.Servers)
	sc.mu.Unlock()
	op := Op{
		Type:        "Join",
		ClientId:    args.ClientId,
		ReqId:       args.ReqId,
		JoinServers: args.Servers,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		select {
		case op := <-sc.getOrCreateOpChannel(index, true):
			if op.ClientId == args.ClientId && op.ReqId == args.ReqId {
				reply.WrongLeader = false
				reply.Err = OK
				return
			}
		case <-time.After(500 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	DPrintf("server is calling leave with leave gids %v\n", args.GIDs)
	sc.mu.Unlock()
	op := Op{
		Type:      "Leave",
		ClientId:  args.ClientId,
		ReqId:     args.ReqId,
		LeaveGids: args.GIDs,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		select {
		case op := <-sc.getOrCreateOpChannel(index, true):
			if op.ClientId == args.ClientId && op.ReqId == args.ReqId {
				reply.WrongLeader = false
				reply.Err = OK
				return
			}
		case <-time.After(500 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      "Move",
		ClientId:  args.ClientId,
		ReqId:     args.ReqId,
		MoveShard: args.Shard,
		MoveGid:   args.GID,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		select {
		case op := <-sc.getOrCreateOpChannel(index, true):
			if op.ClientId == args.ClientId && op.ReqId == args.ReqId {
				reply.WrongLeader = false
				reply.Err = OK
				return
			}
		case <-time.After(500 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	DPrintf("server is calling query, config is %v\n", sc.configs)
	sc.mu.Unlock()
	op := Op{
		Type:     "Query",
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		QueryNum: args.Num,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		DPrintf("server is not leader\n")
		reply.WrongLeader = true
		return
	} else {
		select {
		case op := <-sc.getOrCreateOpChannel(index, true):
			if op.ClientId == args.ClientId && op.ReqId == args.ReqId {
				reply.WrongLeader = false
				reply.Err = OK
				sc.mu.Lock()
				reply.Config = sc.query(op.QueryNum)
				sc.mu.Unlock()
				return
			}
		case <-time.After(500 * time.Millisecond):
			reply.WrongLeader = false
			return
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) balancedShards(prevConfig Config, gidToShard map[int]int) [NShards]int {
	len := len(gidToShard)
	avg := NShards / len
	rem := NShards % len
	sorted := make([]int, 0)
	for k := range gidToShard {
		sorted = append(sorted, k)
	}
	sort.Ints(sorted)
	DPrintf("gidToShard is %v, sorted is %v\n", gidToShard, sorted)
	for i := 0; i < len; i++ {
		res := avg
		sortedGid := sorted[i]
		//	DPrintf("res is %v, rem is %v, gidToShard[sorted[i]] is %v, sortedGid is %v\n", res, rem, gidToShard[sorted[i]], sortedGid)
		if i < rem {
			res++
		}
		if res < gidToShard[sorted[i]] {
			newNum := gidToShard[sortedGid] - res
			for shard, gid := range prevConfig.Shards {
				if newNum <= 0 {
					break
				}
				if gid == sortedGid {
					prevConfig.Shards[shard] = 0
					newNum--
				}
			}
		}
		if res > gidToShard[sorted[i]] {
			newNum := res - gidToShard[sortedGid]
			for shard, gid := range prevConfig.Shards {
				if newNum <= 0 {
					break
				}
				if gid == 0 {
					prevConfig.Shards[shard] = sortedGid
					newNum--
				}
			}
		}
	}

	DPrintf("prevConfig shards is %v\n", prevConfig.Shards)
	return prevConfig.Shards
}

func (sc *ShardCtrler) join(servers map[int][]string) {
	prevConfig := sc.configs[len(sc.configs)-1]
	config := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}
	for gid, server := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[gid] = server
	}
	for gid, server := range servers {
		config.Groups[gid] = server
	}
	gidToShard := make(map[int]int)
	for gid := range config.Groups {
		gidToShard[gid] = 0
	}
	for _, gid := range prevConfig.Shards {
		if gid != 0 {
			gidToShard[gid]++
		}
	}
	config.Shards = sc.balancedShards(prevConfig, gidToShard)
	sc.configs = append(sc.configs, config)
}
func (sc *ShardCtrler) leave(gids []int) {
	prevConfig := sc.configs[len(sc.configs)-1]
	config := Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	for gid, server := range prevConfig.Groups {
		config.Groups[gid] = server
	}
	leaveGids := make(map[int]bool)
	for _, gid := range gids {
		leaveGids[gid] = true
	}
	for _, gid := range gids {
		delete(config.Groups, gid)
	}
	gidsToShard := make(map[int]int)
	for shard, gid := range prevConfig.Shards {
		if gid != 0 {
			if leaveGids[gid] {
				prevConfig.Shards[shard] = 0
			} else {
				gidsToShard[gid]++
			}
		}
	}
	if len(config.Groups) != 0 {
		config.Shards = sc.balancedShards(prevConfig, gidsToShard)
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) move(shard int, gid int) {
	config := Config{
		Num:    len(sc.configs),
		Shards: sc.configs[len(sc.configs)-1].Shards,
		Groups: make(map[int][]string),
	}
	for key, value := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[key] = value
	}
	config.Shards[shard] = gid
	sc.configs = append(sc.configs, config)
}
func (sc *ShardCtrler) query(num int) Config {
	if num == -1 || num > len(sc.configs)-1 {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[num]
	}
}

func (sc *ShardCtrler) commandApplier() {
	for {
		msg := <-sc.applyCh
		if msg.CommandValid {
			sc.mu.Lock()
			op := msg.Command.(Op)
			reqId, exists := sc.duplicateMap[op.ClientId]
			if !exists || reqId < op.ReqId {
				switch op.Type {
				case "Join":
					sc.join(op.JoinServers)
				case "Leave":
					sc.leave(op.LeaveGids)
				case "Move":
					sc.move(op.MoveShard, op.MoveGid)
				case "Query":
					sc.query(op.QueryNum)
				}
				sc.duplicateMap[op.ClientId] = op.ReqId
				sc.lastSnapshotIndex = msg.CommandIndex
			}
			sc.getOrCreateOpChannel(msg.CommandIndex, false) <- op
			sc.mu.Unlock()
		} else if msg.SnapshotValid {
			// handle snapshots
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.duplicateMap = make(map[int64]int64)
	sc.waitMap = make(map[int]chan Op)
	sc.lastSnapshotIndex = 0

	go sc.commandApplier()
	return sc
}
