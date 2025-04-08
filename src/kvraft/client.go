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

	msgsToDeliver []MessageToDeliver
	msgLock       sync.Mutex
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
	// You'll have to add code here.
	ck.logger = logger.NewLogger(ck.clerkId, true, "Clerk", constants.ClerkLoggingMap)

	// thread that loops through messages, checks if highest sequence number == message seq number - 1, deliver to client
	go ck.Applier()
	return ck
}

func (ck *Clerk) Applier() {
	// TODO: when to break out of this? add killed?
	for {
		ck.msgLock.Lock()
		for i, msg := range ck.msgsToDeliver {
			if msg.SeqNo == ck.deliverSequenceNo+1 {
				ck.deliverSequenceNo++
				msg.OutChan <- msg.Value
				ck.msgsToDeliver = append(ck.msgsToDeliver[:i], ck.msgsToDeliver[i+1:]...)
			}
		}

		ck.msgLock.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	args := GetArgs{Key: key}

	ck.msgLock.Lock()
	ck.sendSequenceNo++
	args.SeqNo = ck.sendSequenceNo
	ck.msgLock.Unlock()

	var reply GetReply

	// You will have to modify this function.
	for {
		serverId := nrand() % int64(len(ck.servers))
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			break
		}

		reply = GetReply{} // clear if err was bad
	}

	OutChan := make(chan string)

	msg := MessageToDeliver{SeqNo: reply.SeqNo, Value: reply.Value, OutChan: OutChan}

	ck.msgLock.Lock()
	ck.msgsToDeliver = append(ck.msgsToDeliver, msg)
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
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	ck.msgLock.Lock()
	ck.sendSequenceNo++
	args.SeqNo = ck.sendSequenceNo
	ck.msgLock.Unlock()

	var reply PutAppendReply

	for {
		serverId := nrand() % int64(len(ck.servers))
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			break
		}
		reply = PutAppendReply{}
	}

	OutChan := make(chan string)

	msg := MessageToDeliver{SeqNo: reply.SeqNo, Value: "", OutChan: OutChan}

	ck.msgLock.Lock()
	ck.msgsToDeliver = append(ck.msgsToDeliver, msg)
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
