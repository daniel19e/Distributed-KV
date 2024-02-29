package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type     string
	Key      string
	Value    string
	ClientId int64
	ReqId    int64
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	waitMap      map[int]chan Op

	kvMap             map[string]string // key value storage
	duplicateMap      map[int64]int64   // map client id to req id
	lastSnapshotIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		ReqId:    args.ReqId}

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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) getOrCreateOpChannel(index int, atomic bool) chan Op {
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

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastSnapshotIndex = 0
	kv.kvMap = make(map[string]string)
	kv.duplicateMap = make(map[int64]int64)
	kv.waitMap = make(map[int]chan Op)

	kv.readSnapshot(persister.ReadSnapshot())
	go kv.snapshotApplier(persister, maxraftstate)
	go kv.commandApplier()

	return kv
}

func (kv *KVServer) commandApplier() {
	for !kv.killed() {
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
func (kv *KVServer) snapshotApplier(persister *raft.Persister, maxraftstate int) {
	if maxraftstate == -1 {
		// no need to take snapshot
		return
	}
	for !kv.killed() {
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
func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvMap)
	d.Decode(&kv.duplicateMap)
	d.Decode(&kv.lastSnapshotIndex)
}
