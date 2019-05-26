package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
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
	opt      OpTypes
	key      string
	val      string
	clientID int64
	seqNum   int
}

type KVServer struct {
	mu            sync.Mutex
	cond          *sync.Cond
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	currCommitIdx int
	currOp        Op
	maxraftstate  int // snapshot if log grows this big

	clientLookup      map[int64]*LatestRPCByClient
	kvStorage         map[string]string
	commitIndexNotify map[int]*CommitIndexCommunication
}

type CommitIndexCommunication struct {
	handlerFinishedWg sync.WaitGroup
	indexUpdatedChan  chan bool
}

type LatestRPCByClient struct {
	seqNum int
	value  string
	err    Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{}
	op.opt = opGet
	op.key = args.Key
	op.clientID = args.ClientID
	op.seqNum = args.SeqNum
	wrongLeader, value, err := kv.requestHandler(&op)
	reply.WrongLeader = wrongLeader
	reply.Value = value
	reply.Err = err
	//kv.rf.Start()
}

func (kv *KVServer) requestHandler(op *Op) (bool, string, Err) {
	// kv.mu.Lock()
	// _, isLeader := kv.rf.GetState()
	// kv.mu.Unlock()
	// if !isLeader {
	// 	return true, ""
	// }
	// the above has been commented out because Start() will always check if
	// the Raft server being contacted is the leader or not
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.clientLookup[op.clientID]; !ok {
		// a new client, keep track of its seq #
		lastestRPC := LatestRPCByClient{}
		lastestRPC.seqNum = -1
		kv.clientLookup[op.clientID] = &lastestRPC
	}
	if kv.clientLookup[op.clientID].seqNum < op.seqNum {
		// current request not in kv server's table
		idxIfEverCommitted, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			return true, "", "" // Raft believes it is not the leader
		}
		// Start returns fairly quickly
		// Start might actually return telling the server that it is not the leader
		if _, ok := kv.commitIndexNotify[idxIfEverCommitted]; !ok {
			kv.commitIndexNotify[idxIfEverCommitted] = &CommitIndexCommunication{}
		}
		kv.commitIndexNotify[idxIfEverCommitted].handlerFinishedWg.Add(1)
		kv.mu.Unlock()
		// unlock here b/c agreement can take a while
		<-kv.commitIndexNotify[idxIfEverCommitted].indexUpdatedChan // listen routine will close this channel when value arrives
		kv.mu.Lock()                                                // ready to read values from kv storage
		// read the value
		if kv.currOp.clientID != op.clientID || kv.currOp.seqNum != op.seqNum {
			// not my op at this commitIndex, error
			return false, "", "Failed"
		}
		// it was my op
		defer kv.commitIndexNotify[idxIfEverCommitted].handlerFinishedWg.Done()
		// val, ok := kv.kvStorage[op.key] // should be fine which one handler reads from, kvstorage or latestRPC
		// kv.commitIndexNotify[idxIfEverCommitted].handlerFinishedWg.Done()
		// if op.opt == opGet {
		// 	// check if the key exists
		// 	if ok {
		// 		return false, val
		// 	}
		// 	// no such key
		// 	return false, ErrNoKey
		// }
		// // put or append
		// return false, "" // done
	}
	// kvServer has seen this request, and it has been executed (listen routine is responsible for updating clientLookup)
	return false, kv.clientLookup[op.clientID].value, kv.clientLookup[op.clientID].err
	// TODO: does a kvServer have to be leader to respond to duplicate requests?
}

func (kv *KVServer) listenApplyCh() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			// command valid
			op, ok := applyMsg.Command.(Op)
			if ok {
				kv.mu.Lock()
				kv.currCommitIdx++
				kv.currOp = op // allow handlers to check if the op is the same as what they submitted
				if op.seqNum > kv.clientLookup[op.clientID].seqNum {
					// not a duplicate request
					kv.clientLookup[op.clientID].seqNum = op.seqNum
					if op.opt == opGet {
						val, ok := kv.kvStorage[op.key]
						if ok {
							kv.clientLookup[op.clientID].value = val
							kv.clientLookup[op.clientID].err = OK
						} else {
							kv.clientLookup[op.clientID].value = ""
							kv.clientLookup[op.clientID].err = ErrNoKey
						}
					} else if op.opt == opPut {
						kv.kvStorage[op.key] = op.val
						kv.clientLookup[op.clientID].value = ""
						kv.clientLookup[op.clientID].err = OK
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
						kv.clientLookup[op.clientID].value = ""
						kv.clientLookup[op.clientID].err = OK
					}
					// correctly processed, now wake up sleeping
				} else {
					// duplicate request
					// DONT IGNORE, may be 2 requests in log
					// should not execute, but should wake up those waiting for this index
				}
				// wake up sleeping
				close(kv.commitIndexNotify[kv.currCommitIdx].indexUpdatedChan)
				kv.mu.Unlock()
				kv.commitIndexNotify[kv.currCommitIdx].handlerFinishedWg.Wait()
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
	wrongLeader, _, err := kv.requestHandler(&op)
	reply.WrongLeader = wrongLeader
	reply.Err = err

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
	kv.clientLookup = make(map[int64]*LatestRPCByClient)
	kv.kvStorage = make(map[string]string)
	kv.commitIndexNotify = make(map[int]*CommitIndexCommunication)
	kv.currCommitIdx = -1

	go kv.listenApplyCh()
	return kv
}
