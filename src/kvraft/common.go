package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	SeqNo   int
	ClerkId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err   Err
	SeqNo int
}

type GetArgs struct {
	Key     string
	SeqNo   int
	ClerkId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	SeqNo int
}

type MessageToDeliver struct {
	SeqNo   int
	Value   string
	OutChan chan string
}
