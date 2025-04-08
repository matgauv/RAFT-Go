package kvraft

import (
	"crypto/rand"
	"lab5/constants"
	"lab5/labrpc"
	"lab5/logger"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	logger  *logger.Logger
	clerkId int
	// You will have to modify this struct.

	sendSequenceNo    int // highest sequence number we have sent
	deliverSequenceNo int // highest sequence number we have recieved

	msgsToDeliver   map[int]MessageToDeliver
	msgLock         sync.Mutex
	lastLeaderIndex int
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
	ck.deliverSequenceNo = -1
	ck.clerkId = int(nrand())
	ck.msgsToDeliver = make(map[int]MessageToDeliver)
	// You'll have to add code here.
	ck.logger = logger.NewLogger(ck.clerkId, true, "Clerk", constants.ClerkLoggingMap)
	ck.lastLeaderIndex = -1

	// thread that loops through messages, checks if highest sequence number == message seq number - 1, deliver to client
	go ck.Applier()
	return ck
}

func (ck *Clerk) Applier() {
	for {
		ck.msgLock.Lock()
		nextSeq := ck.deliverSequenceNo + 1
		if msg, ok := ck.msgsToDeliver[nextSeq]; ok {
			delete(ck.msgsToDeliver, nextSeq)
			ck.deliverSequenceNo = nextSeq
			msg.OutChan <- msg.Value
		}
		ck.msgLock.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
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
	args := GetArgs{Key: key, ClerkId: ck.clerkId}

	ck.msgLock.Lock()
	ck.sendSequenceNo++
	args.SeqNo = ck.sendSequenceNo
	ck.msgLock.Unlock()

	var reply GetReply

	// You will have to modify this function.
	for {
		var serverId int
		if ck.lastLeaderIndex != -1 {
			serverId = ck.lastLeaderIndex
		} else {
			serverId = int(nrand() % int64(len(ck.servers)))
		}

		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeaderIndex = serverId
			break
		}

		ck.lastLeaderIndex = -1
		reply = GetReply{} // clear if err was bad
	}

	OutChan := make(chan string)

	msg := MessageToDeliver{SeqNo: reply.SeqNo, Value: reply.Value, OutChan: OutChan}

	ck.msgLock.Lock()
	ck.msgsToDeliver[msg.SeqNo] = msg
	ck.msgLock.Unlock()

	return <-OutChan
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
	args := PutAppendArgs{Key: key, ClerkId: ck.clerkId, Value: value, Op: op}
	ck.msgLock.Lock()
	ck.sendSequenceNo++
	args.SeqNo = ck.sendSequenceNo
	ck.msgLock.Unlock()

	var reply PutAppendReply

	for {
		var serverId int
		if ck.lastLeaderIndex != -1 {
			serverId = ck.lastLeaderIndex
		} else {
			serverId = int(nrand() % int64(len(ck.servers)))
		}

		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeaderIndex = serverId
			break
		}

		ck.lastLeaderIndex = -1
		reply = PutAppendReply{}
	}

	OutChan := make(chan string)

	msg := MessageToDeliver{SeqNo: reply.SeqNo, Value: "", OutChan: OutChan}

	ck.msgLock.Lock()
	ck.msgsToDeliver[msg.SeqNo] = msg
	ck.msgLock.Unlock()

	<-OutChan
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
