package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) printShardMapToString(shardMap map[int]*Shard) string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "SERVER %d-%d: shardMap {", kv.gid, kv.me)
	for i, shard := range shardMap {
		fmt.Fprintf(&sb, "%d: %v ", i, shard)
	}
	fmt.Fprintf(&sb, "}")
	return sb.String()
}

const (
	Put         = "Put"
	Append      = "Append"
	Get         = "Get"
	Config      = "Config"
	MoveShards  = "MoveShards"
	RemoveShard = "RemoveShard"
	WrongGroup  = "WrongGroup"
)

type OpType string

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientId int64
	ReqId    int

	// for shard transfer
	Config       shardctrler.Config
	ConfigNum    int
	Shard        int
	Storage      map[string]string
	DuplicateMap map[int64]int
}

const (
	Ready = iota
	Waiting
)

type ShardState int
type Shard struct {
	Storage   map[string]string
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	sctrler      *shardctrler.Clerk

	storage        map[string]string
	waitMap        map[int]chan Op
	duplicateMap   map[int64]int
	lastAppliedMsg int
	config         shardctrler.Config
	// shard update related state
	activeShards   map[int]bool
	outgoingShards map[int]map[int]map[string]string //shard -> (config num -> storage)
	incomingShards map[int]int                       //shard -> config num
	garbage        map[int]map[int]bool              // config num -> (shard -> isGarbage)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     Get,
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	reply.Value, reply.Err = kv.sendToRaft(op)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	_, reply.Err = kv.sendToRaft(op)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MoveShardsArgs{})
	labgob.Register(MoveShardsReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	DPrintf("SERVER %d start, gid %d", me, gid)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sctrler = shardctrler.MakeClerk(kv.ctrlers)
	kv.duplicateMap = make(map[int64]int)
	kv.waitMap = make(map[int]chan Op)
	kv.config = shardctrler.Config{}
	kv.lastAppliedMsg = 0
	kv.storage = make(map[string]string)

	kv.outgoingShards = make(map[int]map[int]map[string]string)
	kv.incomingShards = make(map[int]int)
	kv.activeShards = make(map[int]bool)
	kv.garbage = make(map[int]map[int]bool)

	kv.readSnapshot(persister.ReadSnapshot())
	go kv.snapshotApplier(persister, maxraftstate)
	kv.startBackgroundTask(kv.commandApplier /*timeout =*/, 10)
	kv.startBackgroundTask(kv.configFetcher /*timeout =*/, 50)
	kv.startBackgroundTask(kv.shardSender /*timeout =*/, 80)
	kv.startBackgroundTask(kv.garbageCollector /*timeout =*/, 200)

	return kv
}

func (kv *ShardKV) startBackgroundTask(function func(), timeout time.Duration) {
	go func() {
		for {
			function()
			time.Sleep(timeout * time.Millisecond)
		}
	}()
}
func (kv *ShardKV) getOrCreateOpChannel(index int, createIfNotExists bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exists := kv.waitMap[index]
	if !exists {
		if !createIfNotExists {
			return nil
		}
		ch = make(chan Op, 1)
		kv.waitMap[index] = ch
	}
	return ch
}

func (kv *ShardKV) sendToRaft(op Op) (string, Err) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	} else {
		ch := kv.getOrCreateOpChannel(index, true)
		select {
		case msg, ok := <-ch:
			if msg.Type == WrongGroup {
				return "", ErrWrongGroup
			}
			if ok {
				close(ch)
			}
			kv.mu.Lock()
			delete(kv.waitMap, index)
			kv.mu.Unlock()
			if op.ClientId == msg.ClientId && op.ReqId == msg.ReqId {
				return msg.Value, OK
			} else {
				return "", ErrWrongLeader
			}
		case <-time.After(1000 * time.Millisecond):
			return "", ErrWrongLeader
		}
	}
}

func (kv *ShardKV) applyConfig(op Op) {
	// update latest config
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.ConfigNum <= kv.config.Num {
		return // ignore outdated config
	}
	prevConfig := kv.config
	kv.config = op.Config
	outgoingShards := kv.activeShards
	kv.activeShards = make(map[int]bool)
	for shard, gid := range op.Config.Shards {
		if gid != kv.gid {
			continue // ignore other groups
		}
		_, ok := outgoingShards[shard]
		if ok || prevConfig.Num == 0 {
			kv.activeShards[shard] = true
			delete(outgoingShards, shard)
		} else {
			kv.incomingShards[shard] = prevConfig.Num
		}
	}
	if len(outgoingShards) > 0 {
		kv.outgoingShards[prevConfig.Num] = make(map[int]map[string]string)
		for shard := range outgoingShards {
			outgoingStorage := make(map[string]string)
			for k, v := range kv.storage {
				if key2shard(k) == shard {
					outgoingStorage[k] = v
					delete(kv.storage, k)
				}
			}
			kv.outgoingShards[prevConfig.Num][shard] = outgoingStorage
		}
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (kv *ShardKV) applyUpdateShards(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.ConfigNum != kv.config.Num-1 {
		return
	}
	delete(kv.incomingShards, op.Shard)
	if _, ok := kv.activeShards[op.Shard]; !ok {
		kv.activeShards[op.Shard] = true
		for k, v := range op.Storage {
			kv.storage[k] = v
		}
		for k, v := range op.DuplicateMap {
			kv.duplicateMap[k] = Max(v, kv.duplicateMap[k]) // only update with the latest reqId
		}
		if _, ok := kv.garbage[op.ConfigNum]; !ok {
			kv.garbage[op.ConfigNum] = make(map[int]bool)
		}
		kv.garbage[op.ConfigNum][op.Shard] = true
	}
}

func (kv *ShardKV) applyPutAppend(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(op.Key)
	reqId, exists := kv.duplicateMap[op.ClientId]
	if !exists || reqId < op.ReqId {
		DPrintf("SERVER %d-%d: appending %s:%s in shard %d\n", kv.gid, kv.me, op.Key, op.Value, shard)
		if op.Type == Put {
			kv.storage[op.Key] = op.Value
		} else if op.Type == Append {
			kv.storage[op.Key] += op.Value
		}
	}
	kv.duplicateMap[op.ClientId] = op.ReqId
}
func (kv *ShardKV) applyGC(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.outgoingShards[op.ConfigNum]; ok {
		delete(kv.outgoingShards[op.ConfigNum], op.Shard)
		if len(kv.outgoingShards[op.ConfigNum]) == 0 {
			delete(kv.outgoingShards, op.ConfigNum)
		}
	}

}

func (kv *ShardKV) commandApplier() {
	msg := <-kv.applyCh
	if msg.CommandValid {
		idx := msg.CommandIndex
		op := msg.Command.(Op)
		switch op.Type {
		case Config:
			kv.applyConfig(op)
			DPrintf("SERVER %d-%d: updating config %v to %v\n", kv.gid, kv.me, kv.config, op.Config)
		case MoveShards:
			kv.applyUpdateShards(op)
			DPrintf("SERVER %d-%d: updating shards %v\n", kv.gid, kv.me, op.Shard)
		case RemoveShard:
			kv.applyGC(op)
		case Put:
			DPrintf("SERVER %d-%d: putting %s:%s in shard %d\n", kv.gid, kv.me, op.Key, op.Value, key2shard(op.Key))
			kv.mu.Lock()
			if _, ok := kv.activeShards[key2shard(op.Key)]; !ok {
				op.Type = WrongGroup
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			kv.applyPutAppend(op)
		case Append:
			DPrintf("SERVER %d-%d: appending %s:%s in shard %d\n", kv.gid, kv.me, op.Key, op.Value, key2shard(op.Key))
			kv.mu.Lock()
			if _, ok := kv.activeShards[key2shard(op.Key)]; !ok {
				op.Type = WrongGroup
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			kv.applyPutAppend(op)
		case Get:
			DPrintf("SERVER %d-%d: getting %s in shard %d\n", kv.gid, kv.me, op.Key, key2shard(op.Key))
			kv.mu.Lock()
			if _, ok := kv.activeShards[key2shard(op.Key)]; !ok {
				op.Type = WrongGroup
				kv.mu.Unlock()
				return
			}
			// no need to update anything
			op.Value = kv.storage[op.Key]
			kv.duplicateMap[op.ClientId] = op.ReqId
			kv.mu.Unlock()
		default:
			panic(fmt.Sprintf("invalid op type: %v", op.Type))
		}
		kv.mu.Lock()
		kv.lastAppliedMsg = idx
		kv.mu.Unlock()
		ch := kv.getOrCreateOpChannel(idx, false)
		if ch != nil {
			select {
			case <-ch:
			default:
			}
			ch <- op
		}
	} else if msg.SnapshotValid {
		kv.mu.Lock()
		if msg.SnapshotIndex <= kv.lastAppliedMsg {
			kv.mu.Unlock()
			return
		}
		kv.readSnapshot(msg.Snapshot)
		kv.lastAppliedMsg = msg.SnapshotIndex
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) snapshotApplier(persister *raft.Persister, maxraftstate int) {
	if maxraftstate == -1 {
		// no need to take snapshot
		return
	}
	for {
		kv.mu.Lock()
		size := persister.RaftStateSize()
		if size > maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.storage)
			e.Encode(kv.duplicateMap)
			e.Encode(kv.lastAppliedMsg)
			e.Encode(kv.config)
			e.Encode(kv.outgoingShards)
			e.Encode(kv.activeShards)
			e.Encode(kv.incomingShards)
			e.Encode(kv.garbage)
			data := w.Bytes()
			kv.rf.Snapshot(kv.lastAppliedMsg, data)
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.storage)
	d.Decode(&kv.duplicateMap)
	d.Decode(&kv.lastAppliedMsg)
	d.Decode(&kv.config)
	d.Decode(&kv.outgoingShards)
	d.Decode(&kv.activeShards)
	d.Decode(&kv.incomingShards)
	d.Decode(&kv.garbage)
}

func (kv *ShardKV) configFetcher() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.incomingShards) > 0 {
		kv.mu.Unlock()
		return
	}
	next := kv.config.Num + 1
	kv.mu.Unlock()
	config := kv.sctrler.Query(next)
	if config.Num == next {
		op := Op{
			Type:      Config,
			Config:    config,
			ConfigNum: config.Num,
		}
		kv.rf.Start(op)
	}
}

func (kv *ShardKV) shardSender() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.incomingShards) == 0 {
		kv.mu.Unlock()
		return
	}
	wg := sync.WaitGroup{}
	for shard, idx := range kv.incomingShards {
		wg.Add(1)
		go func(shard int, config shardctrler.Config) {
			defer wg.Done()
			args := MoveShardsArgs{shard, config.Num}
			gid := config.Shards[shard]
			for _, server := range config.Groups[gid] {
				srv := kv.make_end(server)
				reply := MoveShardsReply{}
				ok := srv.Call("ShardKV.MoveShardsRPC", &args, &reply)
				if ok && reply.Err == OK {
					op := Op{
						Type:         MoveShards,
						ConfigNum:    reply.ConfigNum,
						Shard:        reply.Shard,
						Storage:      reply.Storage,
						DuplicateMap: reply.DuplicateMap,
					}
					kv.rf.Start(op)
				}

			}
		}(shard, kv.sctrler.Query(idx))
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) garbageCollector() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbage) == 0 {
		kv.mu.Unlock()
		return
	}
	wg := sync.WaitGroup{}
	for configNum, shards := range kv.garbage {
		for shard := range shards {
			wg.Add(1)
			go func(shard int, config shardctrler.Config, configNum int) {
				defer wg.Done()
				args := MoveShardsArgs{Shard: shard, ConfigNum: config.Num}
				gid := config.Shards[shard]
				for _, server := range config.Groups[gid] {
					srv := kv.make_end(server)
					reply := MoveShardsReply{}
					ok := srv.Call("ShardKV.GarbageCollectionRPC", &args, &reply)
					if ok && reply.Err == OK {
						kv.mu.Lock()
						defer kv.mu.Unlock()
						delete(kv.garbage[configNum], shard)
						if len(kv.garbage[configNum]) == 0 {
							delete(kv.garbage, configNum)
						}
					}
				}
			}(shard, kv.sctrler.Query(configNum), configNum)
		}
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) MoveShardsRPC(args *MoveShardsArgs, reply *MoveShardsReply) {
	reply.Err = ErrWrongLeader
	reply.Shard = args.Shard
	reply.ConfigNum = args.ConfigNum
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if !isLeader || args.ConfigNum >= kv.config.Num {
		return
	}
	reply.Err = OK
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	reply.Storage = make(map[string]string)
	reply.DuplicateMap = make(map[int64]int)
	// need to copy all the data into the rpc reply
	for k, v := range kv.outgoingShards[args.ConfigNum][args.Shard] {
		reply.Storage[k] = v
	}
	for k, v := range kv.duplicateMap {
		reply.DuplicateMap[k] = v
	}
}

func (kv *ShardKV) GarbageCollectionRPC(args *MoveShardsArgs, reply *MoveShardsReply) {
	reply.Err = ErrWrongLeader
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	shards, ok := kv.outgoingShards[args.ConfigNum]
	if !isLeader || !ok || shards[args.Shard] == nil {
		kv.mu.Unlock()
		return
	}
	op := Op{
		Type:      RemoveShard,
		ConfigNum: args.ConfigNum,
		Shard:     args.Shard,
	}
	kv.mu.Unlock()
	_, reply.Err = kv.sendToRaft(op)
}
