package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	leaderId int64 // keep track of current leader
	reqId    int64
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.reqId = 0
	return ck
}

func (ck *Clerk) updateLeaderId(leader int64) {
	ck.mu.Lock()
	ck.leaderId = leader
	ck.mu.Unlock()
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("Clerk(%d) - req(%d) get %s\n", ck.clientId, ck.reqId, key)
	// atomically update leader and reqId, then call Get RPC
	ck.mu.Lock()
	ck.reqId++
	leader := ck.leaderId
	reqId := ck.reqId
	ck.mu.Unlock()
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		ReqId:    reqId,
	}

	// retry indefinitely until get a valid reply
	val := ""
	for {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			if reply.Err == OK {
				DPrintf("updating return val\n")
				val = reply.Value
			}
			break
		}
		leader = (leader + 1) % int64(len(ck.servers))
	}
	ck.updateLeaderId(leader)
	DPrintf("get return val = %s\n", val)
	return val
}

// shared by Put and Append.
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("Clerk(%d) - req(%d) putappend %s %s\n", ck.clientId, ck.reqId, key, value)
	// atomically update leader and reqId, then call PutAppend RPC
	ck.mu.Lock()
	ck.reqId++
	leader := ck.leaderId
	reqId := ck.reqId
	ck.mu.Unlock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		ReqId:    reqId,
	}
	// retry indefinitely until get a valid reply
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			break
		}
		leader = (leader + 1) % int64(len(ck.servers))
	}

	ck.updateLeaderId(leader)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
