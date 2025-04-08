package kvraft

import (
	"crypto/rand"
	"lab5/constants"
	"lab5/labrpc"
	"lab5/logger"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	logger  *logger.Logger
	clerkId int
	// You will have to modify this struct.

	sendSequenceNo int // highest sequence number we have sent

	lastLeaderIndex int
	mu              sync.Mutex
}

func nrand() int64 {
	max_int := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max_int)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.sendSequenceNo = -1
	ck.clerkId = int(nrand())
	// You'll have to add code here.
	ck.logger = logger.NewLogger(ck.clerkId, true, "Clerk", constants.ClerkLoggingMap)
	ck.lastLeaderIndex = -1

	// thread that loops through messages, checks if highest sequence number == message seq number - 1, deliver to client
	return ck
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

	ck.mu.Lock()
	ck.sendSequenceNo++
	seq := ck.sendSequenceNo
	ck.mu.Unlock()

	args := GetArgs{Key: key, ClerkId: ck.clerkId, SeqNo: seq}

	// You will have to modify this function.
	for {
		var reply GetReply

		var serverId int
		if ck.lastLeaderIndex != -1 {
			serverId = ck.lastLeaderIndex
		} else {
			serverId = int(nrand() % int64(len(ck.servers)))
		}

		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeaderIndex = serverId
			return reply.Value
		}

		ck.lastLeaderIndex = -1
	}
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
	// You will have to modify this function.
	ck.mu.Lock()
	ck.sendSequenceNo++
	seq := ck.sendSequenceNo
	ck.mu.Unlock()

	args := PutAppendArgs{Key: key, ClerkId: ck.clerkId, Value: value, Op: op, SeqNo: seq}

	for {
		var reply PutAppendReply
		var serverId int
		if ck.lastLeaderIndex != -1 {
			serverId = ck.lastLeaderIndex
		} else {
			serverId = int(nrand() % int64(len(ck.servers)))
		}

		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeaderIndex = serverId
			return
		}

		ck.lastLeaderIndex = -1
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
