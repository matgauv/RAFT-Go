package kvraft

import (
	"encoding/gob"
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
	Value interface{}
}

type ApplyFromRaft struct {
	value    string
	commited bool
	ch       chan bool
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
	seqNoChanMap map[int]map[int]ApplyFromRaft
	store        map[string]string
	currentTerm  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	val, ok := kv.seqNoChanMap[args.ClerkId][args.SeqNo]
	if ok && val.commited {
		reply.SeqNo = args.SeqNo
		reply.Value = val.value
		kv.mu.Unlock()
		return
	}

	if args.SeqNo != 0 {
		delete(kv.seqNoChanMap[args.ClerkId], args.SeqNo-1)
	}
	kv.mu.Unlock()

	op := Op{Op: "Get", Value: *args}
	_, _, leader := kv.rf.Start(op)
	if !leader {
		reply.Value = ""
		reply.Err = ErrWrongLeader
		reply.SeqNo = args.SeqNo
		return
	}

	kv.mu.Lock()
	_, ok = kv.seqNoChanMap[args.ClerkId]
	if !ok {
		kv.seqNoChanMap[args.ClerkId] = make(map[int]ApplyFromRaft)
	}
	ch := make(chan bool)
	kv.seqNoChanMap[args.ClerkId][args.SeqNo] = ApplyFromRaft{value: "", commited: false, ch: ch}
	kv.mu.Unlock()

	ok = <-ch

	// cancel rpc because we lost leadership
	if !ok {
		reply.Err = ErrWrongLeader
		reply.SeqNo = args.SeqNo
		return
	}

	kv.mu.Lock()
	reply.Value = kv.seqNoChanMap[args.ClerkId][args.SeqNo].value
	kv.mu.Unlock()

	reply.SeqNo = args.SeqNo
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	val, ok := kv.seqNoChanMap[args.ClerkId][args.SeqNo]
	if ok && val.commited {
		reply.SeqNo = args.SeqNo
		kv.mu.Unlock()
		return
	}

	if args.SeqNo != 0 {
		delete(kv.seqNoChanMap[args.ClerkId], args.SeqNo-1)
	}
	kv.mu.Unlock()

	op := Op{Op: "PutAppend", Value: *args}
	_, _, leader := kv.rf.Start(op)
	if !leader {
		reply.Err = ErrWrongLeader
		reply.SeqNo = args.SeqNo
		return
	}
	kv.mu.Lock()
	_, ok = kv.seqNoChanMap[args.ClerkId]
	if !ok {
		kv.seqNoChanMap[args.ClerkId] = make(map[int]ApplyFromRaft)
	}
	ch := make(chan bool)
	kv.seqNoChanMap[args.ClerkId][args.SeqNo] = ApplyFromRaft{value: "", commited: false, ch: ch}
	kv.mu.Unlock()

	ok = <-ch
	if !ok {
		reply.Err = ErrWrongLeader
		reply.SeqNo = args.SeqNo
		return
	}

	reply.SeqNo = args.SeqNo
}

func (kv *KVServer) cancelPendingRPCs() {
	for {
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Lock()
			kv.currentTerm = currentTerm

			for _, clientMap := range kv.seqNoChanMap {
				for _, aop := range clientMap {
					if !aop.commited && aop.ch != nil {
						select {
						case aop.ch <- false:
						default:
						}
					}
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
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
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			op := msg.Command.(Op)
			if op.Op == "Get" {
				args := op.Value.(GetArgs)
				value, ok2 := kv.store[args.Key]
				if !ok2 {
					value = ""
				}
				applyFromRaft, ok1 := kv.seqNoChanMap[args.ClerkId][args.SeqNo]
				if ok1 {
					applyFromRaft.commited = true
					applyFromRaft.value = value
					kv.seqNoChanMap[args.ClerkId][args.SeqNo] = applyFromRaft

					applyFromRaft.ch <- true
				} else {
					_, ok := kv.seqNoChanMap[args.ClerkId]
					if !ok {
						kv.seqNoChanMap[args.ClerkId] = make(map[int]ApplyFromRaft)
					}
					kv.seqNoChanMap[args.ClerkId][args.SeqNo] = ApplyFromRaft{value: value, commited: true, ch: nil}
				}
			} else if op.Op == "PutAppend" {
				args := op.Value.(PutAppendArgs)

				if args.Op == "Put" {
					kv.store[args.Key] = args.Value
				} else if args.Op == "Append" {
					val, ok := kv.store[args.Key]
					if ok {
						kv.store[args.Key] = val + args.Value
					} else {
						kv.store[args.Key] = args.Value
					}
				}

				applyFromRaft, ok := kv.seqNoChanMap[args.ClerkId][args.SeqNo]
				if ok {
					applyFromRaft.commited = true
					kv.seqNoChanMap[args.ClerkId][args.SeqNo] = applyFromRaft

					applyFromRaft.ch <- true
				} else {
					_, ok = kv.seqNoChanMap[args.ClerkId]
					if !ok {
						kv.seqNoChanMap[args.ClerkId] = make(map[int]ApplyFromRaft)
					}
					kv.seqNoChanMap[args.ClerkId][args.SeqNo] = ApplyFromRaft{value: "", commited: true, ch: nil}
				}

			}
			kv.mu.Unlock()
		}
		//time.Sleep(1 * time.Millisecond)
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
	kv.seqNoChanMap = make(map[int]map[int]ApplyFromRaft)
	kv.store = make(map[string]string)
	kv.currentTerm = -1

	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})

	// You may need initialization code here.
	go kv.Applier()
	go kv.cancelPendingRPCs()
	return kv
}
