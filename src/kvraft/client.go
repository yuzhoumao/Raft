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

	ok := false
	for {
		ok = ck.servers[ck.currentLeaderIdx].Call("KVServer.Get", &args, &reply)
		if (ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == ErrNoKey)) {
			break
		}
		ck.currentLeaderIdx = (ck.currentLeaderIdx + 1) % len(ck.servers)
	}
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
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
	
	ok := false
	for {
		ok = ck.servers[ck.currentLeaderIdx].Call("KVServer.PutAppend", &args, &reply)
		if (ok && !reply.WrongLeader && reply.Err == OK) {
			break
		}
		ck.currentLeaderIdx = (ck.currentLeaderIdx + 1) % len(ck.servers)
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
