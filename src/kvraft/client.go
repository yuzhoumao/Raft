package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	currentLeaderIdx int
	clientID int64
	seqNum int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	// You'll have to add code here.
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	
	args.ClientID = ck.clientID
	args.SeqNum = ck.seqNum
	ck.seqNum++
	
	reply := GetReply{}
	
	ck.sendGetUntilReply(&args, &reply)
	for reply.WrongLeader {
		ck.sendGetUntilReply(&args, &reply)
	}
	return reply.Value
}

func (ck *Clerk) sendGetUntilReply(args *GetArgs, reply *GetReply) {
	ok := ck.servers[ck.currentLeaderIdx].Call("KVServer.Get", &args, &reply)
	for !ok {
		// resend?
		ck.currentLeaderIdx++
		ok = ck.servers[ck.currentLeaderIdx].Call("KVServer.Get", &args, &reply)
	}
}

func (ck *Clerk) sendPAUntilReply(args *PutAppendArgs, reply *PutAppendReply) {
	ok := ck.servers[ck.currentLeaderIdx].Call("KVServer.PutAppend", args, reply)
	for !ok {
		// resend?
		ck.currentLeaderIdx++
		ok = ck.servers[ck.currentLeaderIdx].Call("KVServer.PutAppend", args, reply)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}

	args.Key = key
	args.Value = value
	args.Op = op
	
	args.ClientID = ck.clientID
	args.SeqNum = ck.seqNum
	ck.seqNum++
	
	reply := PutAppendReply{}
	
	ck.sendPAUntilReply(&args, &reply)
	for reply.WrongLeader {
		ck.sendPAUntilReply(&args, &reply)
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
