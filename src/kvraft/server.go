package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpTypes int

const (
	opGet OpTypes = iota
	opPut
	opAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	opt OpTypes
	key string
	val string
	clientID int64
	seqNum int
}

type KVServer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	clientLookup map[int64]int
	kvStorage map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{}
	op.opt = opGet
	op.key = args.Key
	op.clientID = args.ClientID
	op.seqNum = args.SeqNum
	wrongLeader, value := kv.requestHandler(&op)
	reply.WrongLeader = wrongLeader
	reply.Value = value
	//kv.rf.Start()
}

func (kv *KVServer) requestHandler(op *Op) (bool, string) {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Unlock()
	if !isLeader {
		return true, "" 
	}
	kv.mu.Lock()
	if _, ok := kv.clientLookup[op.clientID]; !ok {
		kv.clientLookup[op.clientID] = -1
	}
	if kv.clientLookup[op.clientID] < op.seqNum {
		// current request not in kv server's table
		kv.rf.Start(op)
		for {
			if kv.clientLookup[op.clientID] >= op.seqNum {
				// can return now	
				break
			} else {
				kv.cond.Wait()
			}
		}
	}
	kv.mu.Unlock()
	//kv.rf.Start()
	return false, ""
}

func (kv *KVServer) listenApplyCh() {
	for applyMsg := range(kv.applyCh) {
		if applyMsg.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			// command valid
			op, ok := applyMsg.Command.(Op)
			if ok {
				kv.mu.Lock()
				if op.seqNum > kv.clientLookup[op.clientID] {
					// not a duplicate request
					if op.opt == opGet {
						// don't need to do anything
					} else if op.opt == opPut {
						kv.kvStorage[op.key] = op.val
					} else if op.opt == opAppend {
						oldVal, kvReadok := kv.kvStorage[op.key]
						if kvReadok {
							var buffer bytes.Buffer
							buffer.WriteString(oldVal)
							buffer.WriteString(op.val)
							kv.kvStorage[op.key] = buffer.String()
						} else {
							kv.kvStorage[op.key] = op.val
						}
					}
					// correctly processed, now wake up sleeping
					kv.clientLookup[op.clientID] = op.seqNum
				} else {
					// duplicate request
				}
			} else {
				// command is not a Op struct
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{}
	if args.Op == "Put" {
		op.opt = opPut
	} else if args.Op == "Append" {
		op.opt = opAppend
	}
	op.key = args.Key
	op.val = args.Value
	wrongLeader, _ := kv.requestHandler(&op)
	reply.WrongLeader = wrongLeader
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.mu = sync.Mutex{}
	kv.cond = sync.NewCond(&kv.mu)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientLookup = make(map[int64]int)
	kv.kvStorage = make(map[string]string)

	go kv.listenApplyCh()
	return kv
}
