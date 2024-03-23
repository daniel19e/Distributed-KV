package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId    int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ReqId    int64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type UpdateShardsArgs struct {
	Shards       [shardctrler.NShards]int
	DuplicateMap map[int64]int64
}
type UpdateShardsReply struct {
}
