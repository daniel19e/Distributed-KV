package shardkv

import (
	"bytes"
	"log"
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

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientId int64
	ReqId    int64
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

	// Your definitions here.
	sctrler           *shardctrler.Clerk
	kvMap             map[string]string
	waitMap           map[int]chan Op
	duplicateMap      map[int64]int64
	lastSnapshotIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		select {
		case op := <-kv.getOrCreateOpChannel(index /*atomic=*/, true):
			if op.ClientId == args.ClientId && op.ReqId == args.ReqId {
				reply.Err = OK
				reply.Value = func() string {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					val, exists := kv.kvMap[op.Key]
					if exists {
						reply.Err = OK
						return val
					} else {
						reply.Err = ErrNoKey
						return ""
					}
				}()
			} else {
				reply.Err = ErrWrongLeader
			}
		case <-time.After(500 * time.Millisecond):
			reply.Err = ErrWrongLeader
		}
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		select {
		case op := <-kv.getOrCreateOpChannel(index /*atomic=*/, true):
			if op.ClientId == args.ClientId && op.ReqId == args.ReqId {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
		case <-time.After(500 * time.Millisecond):
			reply.Err = ErrWrongLeader
		}
	}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sctrler = shardctrler.MakeClerk(kv.ctrlers)
	kv.kvMap = make(map[string]string)
	kv.duplicateMap = make(map[int64]int64)
	kv.waitMap = make(map[int]chan Op)
	kv.lastSnapshotIndex = 0

	kv.readSnapshot(persister.ReadSnapshot())
	go kv.commandApplier()
	go kv.snapshotApplier(persister, maxraftstate)

	return kv
}

func (kv *ShardKV) commandApplier() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			idx := msg.CommandIndex
			kv.mu.Lock()
			op := msg.Command.(Op)
			reqId, exists := kv.duplicateMap[op.ClientId]
			if !exists || reqId < op.ReqId {
				switch op.Type {
				case "Put":
					kv.kvMap[op.Key] = op.Value
				case "Append":
					kv.kvMap[op.Key] += op.Value
				case "Get":
					// no need to update anything
				}
				// update duplicateMap with last req id
				kv.duplicateMap[op.ClientId] = op.ReqId
				kv.lastSnapshotIndex = idx
			}
			// lock is already held, so no need to do this atomically
			kv.getOrCreateOpChannel(idx /*atomic=*/, false) <- op
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// handle snapshots
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
			e.Encode(kv.kvMap)
			e.Encode(kv.duplicateMap)
			e.Encode(kv.lastSnapshotIndex)
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
	d.Decode(&kv.kvMap)
	d.Decode(&kv.duplicateMap)
	d.Decode(&kv.lastSnapshotIndex)
}
