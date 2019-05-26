package raftkv

import (
	"bytes"
	"fmt"
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
	Opt      OpTypes
	Key      string
	Val      string
	ClientID int64
	SeqNum   int
}

type KVServer struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	currCommitIdx int
	currOp        Op
	maxraftstate  int // snapshot if log grows this big

	clientLookup      map[int64]*LatestRPCByClient      // per client duplicate handling
	kvStorage         map[string]string                 // shared kv state across client
	commitIndexNotify map[int]*CommitIndexCommunication // poke routines as necessary
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
	op.Opt = opGet
	op.Key = args.Key
	op.ClientID = args.ClientID
	op.SeqNum = args.SeqNum
	wrongLeader, value, err := kv.requestHandler(&op)
	reply.WrongLeader = wrongLeader
	reply.Value = value
	reply.Err = err
	//kv.rf.Start()
}

func (kv *KVServer) requestHandler(op *Op) (bool, string, Err) {
	DPrintf("inside requestHandler")
	// kv.mu.Lock()
	// _, isLeader := kv.rf.GetState()
	// kv.mu.Unlock()
	// if !isLeader {
	// 	return true, ""
	// }
	// the above has been commented out because Start() will always check if
	// the Raft server being contacted is the leader or not
	kv.mu.Lock()
	fmt.Printf("90 kvServer # %d got the lock\n", kv.me)
	defer fmt.Printf("kvServer # %d released the lock\n", kv.me)
	defer kv.mu.Unlock()
	if _, ok := kv.clientLookup[op.ClientID]; !ok {
		// a new client, keep track of its seq #
		DPrintf("this is a new client")
		lastestRPC := LatestRPCByClient{}
		lastestRPC.seqNum = -1
		kv.clientLookup[op.ClientID] = &lastestRPC
	}
	if kv.clientLookup[op.ClientID].seqNum < op.SeqNum {
		// current request not in kv server's table
		idxIfEverCommitted, _, isLeader := kv.rf.Start(*op)
		fmt.Printf("kvServer # %d returned from Start\n", kv.me)
		DPrintf("idxIfEverCommitted %d", idxIfEverCommitted)
		if !isLeader {
			DPrintf("Raft believes it is not the leader")
			return true, "", "" // Raft believes it is not the leader
		}
		fmt.Printf("kvserver %d starting op for %v %v\n", kv.me, op.ClientID, op.SeqNum)
		// Start returns fairly quickly
		// Start might actually return telling the server that it is not the leader
		if _, ok := kv.commitIndexNotify[idxIfEverCommitted]; !ok {
			DPrintf("new commit index %d", idxIfEverCommitted)
			kv.commitIndexNotify[idxIfEverCommitted] = &CommitIndexCommunication{}
			kv.commitIndexNotify[idxIfEverCommitted].indexUpdatedChan = make(chan bool)
		}
		kv.commitIndexNotify[idxIfEverCommitted].handlerFinishedWg.Add(1)
		fmt.Printf("kvServer # %d released the lock\n", kv.me)
		kv.mu.Unlock()
		// unlock here b/c agreement can take a while
		<-kv.commitIndexNotify[idxIfEverCommitted].indexUpdatedChan // listen routine will close this channel when value arrives
		kv.mu.Lock()                                                // ready to read values from kv storage
		fmt.Printf("122 kvServer # %d got the lock\n", kv.me)
		// read the value
		if kv.currOp.ClientID != op.ClientID || kv.currOp.SeqNum != op.SeqNum {
			// not my op at this commitIndex, error
			fmt.Printf("not my op at this commitIndex, error")
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
	fmt.Printf("kvServer # %d returning from handler\n", kv.me)
	// kvServer has seen this request, and it has been executed (listen routine is responsible for updating clientLookup)
	return false, kv.clientLookup[op.ClientID].value, kv.clientLookup[op.ClientID].err
	// TODO: does a kvServer have to be leader to respond to duplicate requests?
}

func (kv *KVServer) listenApplyCh() {
	for {
		applyMsg := <-kv.applyCh
		fmt.Printf("kvServer # %d received a new message from applyCh\n", kv.me)
		if applyMsg.CommandValid == false {
			// ignore other types of ApplyMsg
			fmt.Printf("kvServer # %d command not valid\n", kv.me)
		} else {
			// command valid
			fmt.Printf("kvServer # %d command valid\n", kv.me)
			op, ok := applyMsg.Command.(Op)
			if ok {
				fmt.Printf("ClientId %d\n", op.ClientID)
				kv.mu.Lock()
				fmt.Printf("163 kvServer # %d got the lock\n", kv.me)
				kv.currCommitIdx++ // new commit
				kv.currOp = op     // allow handlers to check if the op is the same as what they submitted
				if _, ok := kv.clientLookup[op.ClientID]; !ok {
					// a new client, keep track of its seq #
					DPrintf("this is a new client")
					lastestRPC := LatestRPCByClient{}
					lastestRPC.seqNum = -1
					kv.clientLookup[op.ClientID] = &lastestRPC
				}
				fmt.Printf("kvServer # %d op.SeqNum %d, kv.clientLookup[op.ClientID].seqNum %d\n", kv.me, op.SeqNum, kv.clientLookup[op.ClientID].seqNum)
				if op.SeqNum > kv.clientLookup[op.ClientID].seqNum {
					fmt.Printf("kvServer # %d not a duplicate request\n", kv.me)
					// not a duplicate request
					kv.clientLookup[op.ClientID].seqNum = op.SeqNum
					if op.Opt == opGet {
						val, ok := kv.kvStorage[op.Key]
						if ok {
							kv.clientLookup[op.ClientID].value = val
							kv.clientLookup[op.ClientID].err = OK
						} else {
							kv.clientLookup[op.ClientID].value = ""
							kv.clientLookup[op.ClientID].err = ErrNoKey
						}
					} else if op.Opt == opPut {
						kv.kvStorage[op.Key] = op.Val
						kv.clientLookup[op.ClientID].value = ""
						kv.clientLookup[op.ClientID].err = OK
					} else if op.Opt == opAppend {
						oldVal, kvReadok := kv.kvStorage[op.Key]
						if kvReadok {
							var buffer bytes.Buffer
							buffer.WriteString(oldVal)
							buffer.WriteString(op.Val)
							kv.kvStorage[op.Key] = buffer.String()
						} else {
							// no such key, append functions like put
							kv.kvStorage[op.Key] = op.Val
						}
						kv.clientLookup[op.ClientID].value = ""
						kv.clientLookup[op.ClientID].err = OK
					}
					// correctly processed, now wake up sleeping
				} else {
					// duplicate request
					// DONT simply IGNORE, may be 2 requests in log
					// should not execute, but should wake up those waiting for this index
				}
				if _, ok := kv.commitIndexNotify[kv.currCommitIdx]; ok {
					commitComm := kv.commitIndexNotify[kv.currCommitIdx]
					DPrintf("kvserver %d closing channel for commit index %d", kv.me, kv.currCommitIdx)
					close(commitComm.indexUpdatedChan) // wake up handlers for this index
					fmt.Printf("kvServer # %d released the lock\n", kv.me)
					kv.mu.Unlock()
					DPrintf("kvserver %d waiting for handlers to read value", kv.me)
					commitComm.handlerFinishedWg.Wait() // wait until all handlers have read the value
				} else {
					fmt.Printf("kvServer # %d released the lock\n", kv.me)
					kv.mu.Unlock()
				}
			} else {
				// command is not a Op struct
				fmt.Printf("command is not a Op struct\n")
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{}
	if args.Op == "Put" {
		op.Opt = opPut
	} else if args.Op == "Append" {
		op.Opt = opAppend
	}
	op.Key = args.Key
	op.Val = args.Value
	op.ClientID = args.ClientID
	op.SeqNum = args.SeqNum
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
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientLookup = make(map[int64]*LatestRPCByClient)
	kv.kvStorage = make(map[string]string)
	kv.commitIndexNotify = make(map[int]*CommitIndexCommunication)
	kv.currCommitIdx = 0

	go kv.listenApplyCh()
	return kv
}
