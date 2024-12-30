package kvsrv

type TaskType int

const (
	Pending = iota
	Committed
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	ID    int64
	Type  TaskType
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key  string
	ID   int64
	Type TaskType
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
