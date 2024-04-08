package shardkv

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
	NewErr         = "ErrNew"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId    int
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ReqId    int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardsArgs struct {
	Shard     int
	ConfigNum int
}
type MoveShardsReply struct {
	Err Err

	ConfigNum    int
	Shard        int
	Storage      map[string]string
	DuplicateMap map[int64]int
}
