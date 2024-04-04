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

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) printShardMapToString(shardMap map[int]*Shard) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "SERVER %d (gid: %d): shardMap {", kv.me, kv.gid)
	for i, shard := range shardMap {
		fmt.Fprintf(&sb, "%d: %v ", i, shard)
	}
	fmt.Fprintf(&sb, "}")
	return sb.String()
}

const (
	Put          = "Put"
	Append       = "Append"
	Get          = "Get"
	Config       = "Config"
	UpdateShards = "UpdateShards"
	RemoveShard  = "RemoveShard"
)

type OpType string

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type         string
	Key          string
	Value        string
	ClientId     int64
	ReqId        int64
	Config       shardctrler.Config
	ShardId      int
	Shard        Shard
	DuplicateMap map[int64]int64
}

const (
	Ready = iota
	Waiting
)

type ShardState int
type Shard struct {
	Storage   map[string]string
	ConfigNum int
	State     ShardState
}

type ShardKV struct {
	mu                sync.Mutex
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	make_end          func(string) *labrpc.ClientEnd
	gid               int
	ctrlers           []*labrpc.ClientEnd
	maxraftstate      int // snapshot if log grows this big
	sctrler           *shardctrler.Clerk
	shardMap          map[int]*Shard
	waitMap           map[int]chan Op
	duplicateMap      map[int64]int64
	lastSnapshotIndex int
	lastConfig        shardctrler.Config
	currentConfig     shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if !kv.isResponsibleFor(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	//fmt.Printf("GET %v\n", kv.printShardMapToString(kv.shardMap))

	op := Op{
		Type:     Get,
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	reply.Value, reply.Err = kv.sendToRaft(op)
	kv.mu.Lock()
	if kv.currentConfig.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardMap[key2shard(args.Key)].Storage == nil {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if !kv.isResponsibleFor(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	_, reply.Err = kv.sendToRaft(op)
	kv.mu.Lock()
	if kv.currentConfig.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardMap[key2shard(args.Key)].Storage == nil {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) UpdateShards(args *UpdateShardsArgs, reply *UpdateShardsReply) {
	op := Op{
		Type:         UpdateShards,
		ClientId:     args.ClientId,
		ReqId:        args.ReqId,
		ShardId:      args.ShardId,
		Shard:        args.Shard,
		DuplicateMap: args.DuplicateMap,
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

func (kv *ShardKV) getOrCreateOpChannel(index int, atomic bool) chan Op {
	if atomic {
		kv.mu.Lock()
		defer kv.mu.Unlock()
	}
	ch, exists := kv.waitMap[index]
	if !exists {
		ch = make(chan Op, 1)
		kv.waitMap[index] = ch
	}
	return ch

}

func (kv *ShardKV) isResponsibleFor(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	config := kv.currentConfig
	shard := key2shard(key)
	return config.Shards[shard] == kv.gid
}

func (kv *ShardKV) sendToRaft(op Op) (string, Err) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	} else {
		select {
		case msg := <-kv.getOrCreateOpChannel(index /*atomic=*/, true):
			if op.ClientId == msg.ClientId && op.ReqId == msg.ReqId {
				if op.Type == Get {
					return func() (string, Err) {
						kv.mu.Lock()
						defer kv.mu.Unlock()
						val, exists := kv.shardMap[key2shard(op.Key)].Storage[op.Key]
						if exists {
							return val, OK
						} else {
							return "", ErrNoKey
						}
					}()
				} else {
					return "", OK
				}
			} else {
				return "", ErrWrongLeader
			}
		case <-time.After(500 * time.Millisecond):
			return "", ErrWrongLeader
		}
	}
}

func (kv *ShardKV) fetchLatestConfig() {
	for {
		kv.mu.Lock()
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		currentConfig := kv.currentConfig
		newConfig := kv.sctrler.Query(currentConfig.Num + 1)
		kv.mu.Unlock()
		if newConfig.Num != currentConfig.Num+1 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		op := Op{
			Type:     Config,
			Config:   newConfig,
			ReqId:    int64(newConfig.Num),
			ClientId: int64(kv.gid),
		}
		kv.sendToRaft(op)
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.lastConfig.Shards {
		if gid == kv.gid && kv.currentConfig.Shards[shard] != kv.gid && kv.shardMap[shard].ConfigNum < kv.currentConfig.Num {
			return false
		}
	}
	return true
}
func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.lastConfig.Shards {
		if gid != kv.gid && kv.currentConfig.Shards[shard] == kv.gid && kv.shardMap[shard].ConfigNum < kv.currentConfig.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) sendUpdatedShards() {
	for {
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if !kv.allSent() {
			duplicateMap := make(map[int64]int64)
			for k, v := range kv.duplicateMap {
				duplicateMap[k] = v
			}
			for shard, gid := range kv.lastConfig.Shards {
				if gid == kv.gid && kv.currentConfig.Shards[shard] != kv.gid && kv.shardMap[shard].ConfigNum < kv.currentConfig.Num {
					shardCopy := &Shard{
						Storage:   make(map[string]string),
						ConfigNum: kv.currentConfig.Num,
					}
					for k, v := range kv.shardMap[shard].Storage {
						shardCopy.Storage[k] = v
					}
					if len(shardCopy.Storage) == 0 {
						continue
					}
					args := &UpdateShardsArgs{
						DuplicateMap: duplicateMap,
						ShardId:      shard,
						Shard:        *shardCopy,
						ClientId:     int64(gid),
						ReqId:        int64(kv.currentConfig.Num),
					}
					reply := &UpdateShardsReply{}
					serverNames := kv.currentConfig.Groups[kv.currentConfig.Shards[shard]]
					servers := make([]*labrpc.ClientEnd, len(serverNames))
					for i := 0; i < len(serverNames); i++ {
						servers[i] = kv.make_end(serverNames[i])
					}
					go func(servers []*labrpc.ClientEnd, args *UpdateShardsArgs, reply *UpdateShardsReply) {
						index := 0
						start := time.Now()
						for {
							ok := servers[index].Call("ShardKV.UpdateShards", args, reply)
							if ok && reply.Err == OK || time.Since(start) > 5*time.Second {
								kv.mu.Lock()
								op := Op{
									Type:     RemoveShard,
									ClientId: int64(kv.gid),
									ReqId:    int64(kv.currentConfig.Num),
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.sendToRaft(op)
								break
							}
							index = (index + 1) % len(servers)
						}
					}(servers, args, reply)
				}
			}
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applyConfig(op Op) {
	// update latest config
	if kv.currentConfig.Num >= op.Config.Num {
		return // ignore outdated config
	}
	for shard, gid := range op.Config.Shards {
		// if incoming shard is not currently owned by this group, create a new storage map
		if gid == kv.gid && kv.currentConfig.Shards[shard] == 0 {
			kv.shardMap[shard].Storage = make(map[string]string)
			kv.shardMap[shard].ConfigNum = op.Config.Num
		}
	}
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = op.Config
	DPrintf("SERVER %d (gid %d): updating config %v to %v", kv.me, kv.gid, kv.currentConfig, op.Config)
}

func (kv *ShardKV) applyUpdateShards(op Op) {
	if kv.shardMap[op.ShardId].Storage == nil || op.Shard.ConfigNum < kv.currentConfig.Num {
		return
	}
	DPrintf("SERVER %d (gid %d): updating shard %d: %v", kv.me, kv.gid, op.ShardId, op.Shard)
	shard := &Shard{
		Storage:   make(map[string]string),
		ConfigNum: op.Shard.ConfigNum,
	}
	for k, v := range op.Shard.Storage {
		shard.Storage[k] = v
	}
	kv.shardMap[op.ShardId] = shard
	// need to handle duplicate requests
	for clientId, reqId := range op.DuplicateMap {
		r, exists := kv.duplicateMap[clientId]
		if !exists || r < reqId {
			kv.duplicateMap[clientId] = reqId
		}
	}
}

func (kv *ShardKV) applyPutAppend(op Op) {
	if kv.currentConfig.Shards[key2shard(op.Key)] != kv.gid || kv.shardMap[key2shard(op.Key)].Storage == nil {
		return
	}
	shard := key2shard(op.Key)
	reqId, exists := kv.duplicateMap[op.ClientId]
	if !exists || reqId < op.ReqId {
		DPrintf("SERVER %d (gid %d): appending %s:%s in shard %d", kv.me, kv.gid, op.Key, op.Value, shard)
		if op.Type == Put {
			kv.shardMap[shard].Storage[op.Key] = op.Value
		} else if op.Type == Append {
			kv.shardMap[shard].Storage[op.Key] += op.Value
		}
	}
	kv.duplicateMap[op.ClientId] = op.ReqId
}

func (kv *ShardKV) commandApplier() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			idx := msg.CommandIndex
			kv.mu.Lock()
			op := msg.Command.(Op)
			switch op.Type {
			case Config:
				kv.applyConfig(op)
			case UpdateShards:
				kv.applyUpdateShards(op)
			case RemoveShard:
				if op.ReqId < int64(kv.currentConfig.Num) {
					kv.mu.Unlock()
					continue
				}
				kv.shardMap[op.ShardId].Storage = nil
				kv.shardMap[op.ShardId].ConfigNum = op.Config.Num
			case Put:
				kv.applyPutAppend(op)
			case Append:
				kv.applyPutAppend(op)
			case Get:
				// no need to update anything
				kv.duplicateMap[op.ClientId] = op.ReqId
			default:
				panic(fmt.Sprintf("invalid op type: %v", op.Type))
			}
			kv.lastSnapshotIndex = idx
			kv.getOrCreateOpChannel(idx /*atomic=*/, false) <- op
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex <= kv.lastSnapshotIndex {
				kv.mu.Unlock()
				continue
			}
			kv.readSnapshot(msg.Snapshot)
			kv.lastSnapshotIndex = msg.SnapshotIndex
			kv.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
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
			e.Encode(kv.shardMap)
			e.Encode(kv.duplicateMap)
			e.Encode(kv.lastSnapshotIndex)
			e.Encode(kv.lastConfig)
			e.Encode(kv.currentConfig)
			data := w.Bytes()
			kv.rf.Snapshot(kv.lastSnapshotIndex, data)
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
	d.Decode(&kv.shardMap)
	d.Decode(&kv.duplicateMap)
	d.Decode(&kv.lastSnapshotIndex)
	d.Decode(&kv.lastConfig)
	d.Decode(&kv.currentConfig)
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	DPrintf("server %d start, gid %d", me, gid)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sctrler = shardctrler.MakeClerk(kv.ctrlers)
	kv.duplicateMap = make(map[int64]int64)
	kv.waitMap = make(map[int]chan Op)
	kv.lastConfig = shardctrler.Config{}
	kv.currentConfig = shardctrler.Config{}
	kv.lastSnapshotIndex = 0

	kv.shardMap = make(map[int]*Shard)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardMap[i] = &Shard{Storage: make(map[string]string)}
	}
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.commandApplier()
	go kv.snapshotApplier(persister, maxraftstate)
	go kv.fetchLatestConfig()
	go kv.sendUpdatedShards()

	return kv
}
