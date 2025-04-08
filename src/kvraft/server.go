package kvraft

import (
	"lab5/labgob"
	"lab5/labrpc"
	"lab5/logger"
	"lab5/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string

	SeqNo   int
	ClerkId int
}

// for duplicate detection
type ApplyFromRaft struct {
	SeqNo int
	Value string
}

// holds all the msgs from raft, pass on chanel to rpc waiting
type MsgToApply struct {
	Term  int
	Value string
	Error Err
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int

	logger *logger.Logger
	// Your definitions here.

	// for each client what was the last request we processed (and result for duplicates)
	appliedMap map[int]ApplyFromRaft

	// rpc handlers listen on these channels
	chMap map[int]chan MsgToApply

	store       map[string]string
	currentTerm int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	last, ok := kv.appliedMap[args.ClerkId]
	// assume client sends seqno in order (1 rpc in flight at a time)
	// thus if the seq no is <= the last seq number we already applied, it's a duplicate
	if ok && args.SeqNo <= last.SeqNo {
		reply.SeqNo = args.SeqNo
		reply.Value = last.Value
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	op := Op{ClerkId: args.ClerkId, SeqNo: args.SeqNo, Op: "Get", Key: args.Key}
	index, term, leader := kv.rf.Start(op)
	if !leader {
		reply.Value = ""
		reply.Err = ErrWrongLeader
		reply.SeqNo = args.SeqNo
		return
	}

	kv.mu.Lock()
	if kv.chMap[index] == nil {
		kv.chMap[index] = make(chan MsgToApply, 1)
	}

	// we wait on this specific index to be written to, then check if the term is right
	// if both are correct, we know it was our command  written by our leader
	ch := kv.chMap[index]
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		// if the term is not the term we originally wrote in, we have lost leadership
		if msg.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = msg.Error
			reply.Value = msg.Value
		}
	// retry if taking too long...
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// assume client sends seqno in order (1 rpc in flight at a time)
	// thus if the seq no is <= the last seq number we already applied, it's a duplicate
	last, ok := kv.appliedMap[args.ClerkId]
	if ok && args.SeqNo <= last.SeqNo {
		reply.Err = OK
		reply.SeqNo = args.SeqNo
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{ClerkId: args.ClerkId, SeqNo: args.SeqNo, Op: args.Op, Key: args.Key, Value: args.Value}
	index, term, leader := kv.rf.Start(op)
	if !leader {
		reply.Err = ErrWrongLeader
		reply.SeqNo = args.SeqNo
		return
	}
	kv.mu.Lock()
	if kv.chMap[index] == nil {
		kv.chMap[index] = make(chan MsgToApply, 1)
	}

	// we wait on this specific index to be written to, then check if the term is right
	// if both are correct, we know it was our command  written by our leader
	ch := kv.chMap[index]
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		// if the term is not the term we originally wrote in, we have lost leadership
		if msg.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = msg.Error
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			commandIndex := msg.CommandIndex
			commandTerm := msg.CommandTerm

			kv.mu.Lock()
			last, ok := kv.appliedMap[op.ClerkId]
			// check if duplicate / old request first
			if !ok || op.SeqNo > last.SeqNo {
				result := ""
				if op.Op == "Put" {
					kv.store[op.Key] = op.Value
				} else if op.Op == "Append" {
					curr := kv.store[op.Key]
					kv.store[op.Key] = curr + op.Value
				} else if op.Op == "Get" {
					result = kv.store[op.Key]
				}

				// keep track of everything applied for duplicate detection
				kv.appliedMap[op.ClerkId] = ApplyFromRaft{SeqNo: op.SeqNo, Value: result}
			}
			ch, chExists := kv.chMap[commandIndex]
			if chExists {
				finalVal := ""
				if op.Op == "Get" {
					finalVal = kv.store[op.Key]
				}

				// notify waiting RPC call that something was written at it's pending log index
				notif := MsgToApply{Term: commandTerm, Value: finalVal, Error: OK}
				ch <- notif
				close(ch)
				delete(kv.chMap, commandIndex)
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	//logger := logger.NewLogger(me, )
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.chMap = make(map[int]chan MsgToApply)
	kv.appliedMap = make(map[int]ApplyFromRaft)
	kv.store = make(map[string]string)
	kv.currentTerm = -1

	// You may need initialization code here.
	go kv.Applier()
	return kv
}
