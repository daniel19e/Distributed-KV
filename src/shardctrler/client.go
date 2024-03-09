package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	clientId int64
	leaderId int64
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
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.reqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	ck.reqId++
	//DPrintf("client is calling query with request id %d\n", ck.reqId)
	//leader := ck.leaderId
	clientId := ck.clientId
	reqId := ck.reqId
	ck.mu.Unlock()

	args := &QueryArgs{}
	args.Num = num
	args.ClientId = clientId
	args.ReqId = reqId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			DPrintf("ok and reply wrong leader is %v --- %v\n", ok, reply.WrongLeader)
			if ok && !reply.WrongLeader {
				DPrintf("client received config %v\n", reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	ck.reqId++
	//	DPrintf("client is calling join with request id %d\n", ck.reqId)
	//leader := ck.leaderId
	reqId := ck.reqId
	clientId := ck.clientId
	ck.mu.Unlock()

	args := &JoinArgs{}
	args.Servers = servers
	args.ClientId = clientId
	args.ReqId = reqId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	ck.reqId++
	//DPrintf("client is calling leave with request id %d\n", ck.reqId)
	//leader := ck.leaderId
	reqId := ck.reqId
	clientId := ck.clientId
	ck.mu.Unlock()

	args := &LeaveArgs{}
	args.GIDs = gids
	args.ClientId = clientId
	args.ReqId = reqId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	ck.reqId++
	//DPrintf("client is calling move with request id %d\n", ck.reqId)
	//leader := ck.leaderId
	reqId := ck.reqId
	clientId := ck.clientId
	ck.mu.Unlock()

	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.ClientId = clientId
	args.ReqId = reqId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
