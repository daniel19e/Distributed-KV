package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
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
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// atomically update leader and reqId, then call Get RPC
	ck.mu.Lock()
	ck.reqId++
	leader := ck.leaderId
	args := GetArgs{Key: key, ClientId: ck.clientId, ReqId: ck.reqId}
	ck.mu.Unlock()

	val := ""
	for {
		reply := GetReply{}
		ok := ck.servers[0].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			if reply.Err == OK {
				val = reply.Value
			}
			break
		}
		leader = (leader + 1) % int64(len(ck.servers))
	}

	ck.updateLeaderId(leader)

	return val
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// atomically update leader and reqId, then call PutAppend RPC
	ck.mu.Lock()
	leader := ck.leaderId
	ck.reqId++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, ReqId: ck.reqId}
	ck.mu.Unlock()

	for {
		reply := PutAppendReply{}
		ok := ck.servers[0].Call("KVServer.PutAppend", &args, &reply)
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
