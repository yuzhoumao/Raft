package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
type LogEntry struct {
	Command      interface{}
	TermReceived int
}

type AppendEntriesArgs struct {
	Term              int         // leader's term
	LeaderId          int         // this allows followers to redirect client
	PrevLogIndex      int         // index of log entry in leader's log immediately preceding this new one
	PrevLogTerm       int         // term of the entry at PrevLogIndex
	Entries           []*LogEntry // empty for heartbeats, may be more than one
	LeaderCommitIndex int         // leader's commitIndex
}

type AppendEntriesReply struct {
	Term                int  // follower replies its currentTerm, so that leader can update in slice
	Success             bool // true if follower contained an entry matching PrevLogIndex and PrevLogTerm
	ConflictTerm        int
	StartOfConflictTerm int // combine the two to avoid same conflict term in the range
}

type AppendEntriesRPC struct {
	args      *AppendEntriesArgs
	reply     *AppendEntriesReply
	peerIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg       // each time a new entry is committed to log, send ApplyMsg to applyCh

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm                    int
	votedFor                       int // used in voting decision
	lastVotedTerm				   int
	log                            []*LogEntry
	commitIndex                    int
	lastApplied                    int
	currentState                   RaftServerState
	majorityNeed                   int
	rawAppendEntriesRPCRequest     chan *AppendEntriesRPC
	handledAppendEntriesRPCRequest chan *AppendEntriesRPC
	electionTimerResetted          bool

	// Leader specific data
	nextIndex  []int // initialize to just after the last one in leader's log when elected
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestVoteReplyWrapper struct {
	Reply *RequestVoteReply
	OK    bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("RequestVote() receiver Raft %+v", rf)
	defer DPrintf("RequestVote() receiver Raft %+v", args)
	defer DPrintf("RequestVote() receiver Raft %+v", reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// args.Term >= rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = follower // all server rule: convert to follower
		rf.votedFor = -1 // reset votedFor
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(rf.log[len(rf.log)-1].TermReceived < args.LastLogTerm || (rf.log[len(rf.log)-1].TermReceived == args.LastLogTerm && rf.commitIndex <= args.LastLogIndex)) {
		// at least as uptodate
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, replyChan chan<- *RequestVoteReply) {
	DPrintf("Raft # %d in function sendRequestVote()", rf.me)
	DPrintf("args.Term %d", args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// resend?
		return
	}
	replyChan <- reply
	DPrintf("Raft # %d returning from function sendRequestVote()", rf.me)
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("Raft # %d in function Start()", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != leader {
		return -1, -1, false
	}
	entry := LogEntry{command, rf.currentTerm}
	rf.log = append(rf.log, &entry)
	// Your code here (2B).

	return len(rf.log) - 1, rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.currentState = killed
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastVotedTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	// rf.log
	entry := LogEntry{nil, 0}
	rf.log = make([]*LogEntry, 0)
	rf.log = append(rf.log, &entry)

	// initialize Raft as follower and timeout resetted to allow one timeout period
	// so that raft servers don't timeout together and split votes on start up
	rf.currentState = follower
	rf.electionTimerResetted = true
	// store min majority count for ease of use
	rf.majorityNeed = len(peers)/2 + 1

	// TODO: add other variable inits
	rf.rawAppendEntriesRPCRequest = make(chan *AppendEntriesRPC)
	rf.handledAppendEntriesRPCRequest = make(chan *AppendEntriesRPC)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start main routine
	go rf.Main()
	// end of Raft server instantiation
	return rf
}

// Main
func (rf *Raft) Main() {
	DPrintf("Raft # %d in function Main()", rf.me)
	go rf.electionTimeoutRoutine()
	go rf.respondAppendEntriesRoutine()
	DPrintf("Raft # %d entering applyMsg loop in Main()", rf.me)
	for {
		if rf.currentState == killed {
			return
		}
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) respondAppendEntriesRoutine() {
	DPrintf("Raft # %d in function respondAppendEntriesRoutine()", rf.me)
	for {
		if rf.currentState == killed {
			return
		}
		rpc := <-rf.rawAppendEntriesRPCRequest
		rf.respondAppendEntriesRoutineHelper(rpc)
		rf.handledAppendEntriesRPCRequest <- rpc
	}
}

func (rf *Raft) respondAppendEntriesRoutineHelper(rpc *AppendEntriesRPC) {
	DPrintf("Raft # %d in function respondAppendEntriesRoutineHelper()", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft # %d processing one AE request", rf.me)
	if rpc.args.Term < rf.currentTerm { // the server sending this RPC thinks it is the leader,
		// while it is actually not
		// this may occur when a broken internet connection suddenly comes live
		rpc.reply.Term = rf.currentTerm // the fake leader shall convert to follower after receiving

		rpc.reply.Success = false
		return
	} else if (rpc.args.Term > rf.currentTerm) {
		rf.currentTerm = rpc.args.Term
		rpc.reply.Term = rpc.args.Term
		DPrintf("rf.currentTerm %d, rpc.args.Term %d, rpc.reply.Term %d", rf.currentTerm, rpc.args.Term, rpc.reply.Term)
		rf.currentState = follower // receiver side conversion
		rf.votedFor = -1 // reset vote
		DPrintf("Raft # %d converting to follower in Helper()", rf.me)
		// All Servers: if RPC response contain term > currentTerm
		// convert to follower
	}
	rf.electionTimerResetted = true
	DPrintf("Raft # %d rpc.args.PrevLogIndex %d len(rf.log) %d", rf.me, rpc.args.PrevLogIndex, len(rf.log))
	if rpc.args.PrevLogIndex > len(rf.log)-1 { // len(rf.log) - 1 is the last index in log
		rpc.reply.Success = false
		rpc.reply.ConflictTerm = 0 // force leader to check len(rf.log) - 1 in next RPC
		rpc.reply.StartOfConflictTerm = len(rf.log)
		return
	}
	DPrintf("Raft # %d rpc.args.PrevLogTerm %d rf.log[rpc.args.PrevLogIndex].TermReceived %d", rf.me, rpc.args.PrevLogTerm, rf.log[rpc.args.PrevLogIndex].TermReceived)
	if rpc.args.PrevLogTerm != rf.log[rpc.args.PrevLogIndex].TermReceived {
		conflictIndex := rpc.args.PrevLogIndex
		// does not contain an entry at prevLogIndex
		conflictTerm := rf.log[conflictIndex].TermReceived
		rpc.reply.Success = false
		rpc.reply.ConflictTerm = conflictTerm
		rpc.reply.StartOfConflictTerm = binarySearchFindFirst(rf.log, conflictIndex, conflictTerm)
		return
	}
	// now that the PrevLog entry agrees, delete all entries in rf.log
	// that does not agree with those in rpc.args.entries
	DPrintf("Raft # %d appending after PrevLogIndex %d", rf.me, rpc.args.PrevLogIndex)
	if len(rpc.args.Entries) > 0 {
		rf.log = append(rf.log[0:rpc.args.PrevLogIndex+1], rpc.args.Entries...)
	}
	// now check if commit any entry
	if rpc.args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(rpc.args.LeaderCommitIndex, len(rf.log)-1)
	}
	rpc.reply.Success = true
	DPrintf("Raft # %d returning from Helper()", rf.me)
}

func (rf *Raft) electionTimeoutRoutine() {
	DPrintf("Raft # %d in function electionTimeoutRoutine()", rf.me)
	for {
		rf.mu.Lock()
		DPrintf("Raft # %d electionTimerResetted: %t", rf.me, rf.electionTimerResetted)
		DPrintf("Raft # %d believes it is LEADER: %t, electionTimeoutRoutine()", rf.me, rf.currentState == leader)
		if rf.currentState == killed {
			rf.mu.Unlock()
			return
		}
		if rf.electionTimerResetted || rf.currentState == leader {
			rf.electionTimerResetted = false
			rf.mu.Unlock()
			sleepTime := getElectionSleepDuration()
			DPrintf("Raft # %d sleeping for %s", rf.me, sleepTime)
			time.Sleep(sleepTime)
		} else {
			// timed out while not being a leader
			// convert to candidate if still a follower
			if rf.currentState == follower {
				// convert to candidate
				DPrintf("Raft # %d had election timed out and converting to candidate", rf.me)
				rf.currentState = candidate
			}
			// if election Timeout elapse, start a new election
			if rf.currentState == candidate {
				rf.electionTimerResetted = true
				rf.currentTerm++
				DPrintf("Raft server # %d kicks off an election in term %d", rf.me, rf.currentTerm)
				go rf.kickOffElection()
			}
			rf.mu.Unlock()
		}
	}
}

// kickOffElection should constantly check currentElection and return once < currentElection
// it should also check currentState and return once follower
func (rf *Raft) kickOffElection() {
	DPrintf("Raft # %d in function kickOffElection()", rf.me)
	// send out requestVote RPCs
	replyChan := make(chan *RequestVoteReply)
	args := RequestVoteArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	DPrintf("rf.currentTerm %d", rf.currentTerm)
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].TermReceived
	rf.mu.Unlock()
	currentVoteCounter := 1
	for i := range rf.peers {
		reply := RequestVoteReply{}
		if i != rf.me {
			go rf.sendRequestVote(i, &args, &reply, replyChan)
			DPrintf("Raft # %d sent RequestVote to Peer # %d", rf.me, i)
			// if the RPC times out, the go routine will return false
			// should the RequestVote RPC be sent again?
		}
	}
	for { // request vote response handler
		reply := <-replyChan
		rf.mu.Lock()
		DPrintf("Raft # %d got RequestVote reply from Peer", rf.me)
		if rf.currentTerm > args.Term {
			// already timed out and kick started a new election
			rf.mu.Unlock()
			return
		}
		DPrintf("Vote granted for Raft # %d in term # %d", rf.me, reply.Term)
		DPrintf("Raft # %d is in term # %d", rf.me, rf.currentTerm)
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			currentVoteCounter++
			if currentVoteCounter >= rf.majorityNeed {
				// elected leader
				DPrintf("Raft # %d received enough votes and converted to leader", rf.me)
				rf.currentState = leader
				go rf.leaderRoutine()
				rf.mu.Unlock()
				return
			}
		} else {
			// vote not granted
			// may need to update the term
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.currentState = follower // voting conversion
				rf.votedFor = -1
				rf.electionTimerResetted = true
				// All Servers: if RPC response contain term > currentTerm
				// convert to follower
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock() // don't hold the lock while listening on channel
	}
}

func (rf *Raft) leaderRoutine() {
	DPrintf("Raft # %d in function leaderRoutine()", rf.me)
	replyChan := make(chan *AppendEntriesRPC)
	go rf.appendEntriesSenderHandleResponse(replyChan)
	go rf.sendHeartbeatRoutine(replyChan) // this handles all heartbeats

	for {
		rf.mu.Lock()
		if rf.currentState != leader {
			rf.mu.Unlock()
			return
		}
		for i := range rf.peers {
			if i != rf.me {
				// if last log index >= nextIndex for a follower
				// send AE rpc with log entries starting at nextIndex
				if rf.nextIndex[i] < len(rf.log)-1 {
					go rf.sendRealAppendEntries(i, replyChan)
				}
				// if success update internal record
				// if fail retry with lower index
			}
		}
		rf.mu.Unlock()
		// if there is an N such that N > commitIndex, commit
		// followers will learn about the commit later during RPC handling
		// NOTE: commit is done after Success AE reply, not here
	}
}

func (rf *Raft) sendRealAppendEntries(peerIndex int, replyChan chan *AppendEntriesRPC) {
	DPrintf("Raft # %d in function sendRealAppendEntries()", rf.me)
	// only start if in leader state, should stop if converts to follower
	// should be accompanied by a timer for every server
	args, replyBefore := AppendEntriesArgs{}, AppendEntriesReply{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	DPrintf("LEADER Raft # %d in term %d", rf.me, args.Term)
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[peerIndex] - 1
	DPrintf("LEADER Raft # %d rpc PrevLogIndex %d", rf.me, args.PrevLogIndex)
	args.PrevLogTerm = rf.log[args.PrevLogIndex].TermReceived
	DPrintf("LEADER Raft # %d rpc PrevLogTerm %d", rf.me, args.PrevLogTerm)
	args.Entries = rf.log[args.PrevLogIndex+1:]
	args.LeaderCommitIndex = rf.commitIndex
	rf.mu.Unlock()
	go rf.sendAppendEntriesBoth(peerIndex, &args, &replyBefore, replyChan)
}

func (rf *Raft) sendHeartbeatRoutine(replyChan chan *AppendEntriesRPC) {
	DPrintf("Raft # %d in function sendHeartbeatRoutine()", rf.me)
	for {
		rf.mu.Lock()
		DPrintf("Raft # %d believes it is LEADER: %t, sendHeartbeatRoutine()", rf.me, rf.currentState == leader)
		if rf.currentState != leader {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		DPrintf("LEADER Raft # %d in term %d", rf.me, args.Term)
		args.LeaderId = rf.me
		args.LeaderCommitIndex = rf.commitIndex
		for i := range rf.peers {
			args.PrevLogIndex = rf.nextIndex[i] - 1
			DPrintf("LEADER Raft # %d rpc PrevLogIndex %d", rf.me, args.PrevLogIndex)
			args.PrevLogTerm = rf.log[args.PrevLogIndex].TermReceived
			DPrintf("LEADER Raft # %d rpc PrevLogTerm %d", rf.me, args.PrevLogTerm)
			reply := AppendEntriesReply{}
			if i != rf.me {
				go rf.sendAppendEntriesBoth(i, &args, &reply, replyChan)
			}
		}
		rf.mu.Unlock()
		DPrintf("Raft # %d sent out heart beats and going to sleep", rf.me)
		time.Sleep(getHeartbeatSleepDuration())
	}
}

func (rf *Raft) sendAppendEntriesBoth(peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply, replyChan chan *AppendEntriesRPC) {
	DPrintf("Raft # %d in function sendAppendEntriesBoth()", rf.me)
	ok := rf.peers[peerIndex].Call("Raft.AppendEntriesReceiverHandler", args, reply)
	for !ok {
		// resend?
		DPrintf("Raft # %d packet dropped", rf.me)
		return
	}
	replyChan <- &AppendEntriesRPC{args, reply, peerIndex}
	// only leaders will send heartbeats and only leaders have to handle AERPC responses
	DPrintf("Raft # %d returning from function sendAppendEntriesBoth()", rf.me)
	return
}

// ReceiverHandler takes the reply and sends into the channel
// which is actively listened by respondAppendEntriesRoutine
func (rf *Raft) AppendEntriesReceiverHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// receiver side
	// hand over the RPC to in channel and it will be handled by the routine
	DPrintf("Raft # %d in function AppendEntriesReceiverHandler()", rf.me)
	rf.rawAppendEntriesRPCRequest <- &AppendEntriesRPC{args, reply, rf.me}
	rpc := <-rf.handledAppendEntriesRPCRequest
	reply = rpc.reply
}

func (rf *Raft) appendEntriesSenderHandleResponse(replyChan chan *AppendEntriesRPC) {
	DPrintf("Raft # %d in function appendEntriesSenderHandleResponse()", rf.me)
	for {
		rpc := <-replyChan
		rf.mu.Lock()
		if rf.currentState != leader {
			rf.mu.Unlock()
			return
		}
		if rpc.reply.Term > rf.currentTerm {
			// rf is a dated leader
			rf.currentTerm = rpc.reply.Term
			rf.currentState = follower // sender side conversion
			rf.votedFor = -1
			rf.electionTimerResetted = true
			rf.mu.Unlock()
			return
		}
		DPrintf("Raft # %d rpc.reply.Success %t", rf.me, rpc.reply.Success)
		DPrintf("Raft # %d rpc.reply.Term %d", rf.me, rpc.reply.Term)
		if rpc.reply.Success {
			// successfully appended entries on this peer, update internal storage
			remoteLen := len(rpc.args.Entries)
			rf.nextIndex[rpc.peerIndex] = rpc.args.PrevLogIndex + remoteLen + 1
			DPrintf("Raft # %d rf.nextIndex[%d] : %d", rf.me, rpc.peerIndex, rf.nextIndex[rpc.peerIndex])
			rf.matchIndex[rpc.peerIndex] = rf.nextIndex[rpc.peerIndex] - 1
			if remoteLen-1 > rf.commitIndex {
				// see if commit possible
				go rf.updateLeaderCommitIndex()
			}
		} else {
			// prevLogIndex empty or does not match
			rf.nextIndex[rpc.peerIndex] = rpc.reply.StartOfConflictTerm - 1 // skip those in between
			DPrintf("Raft # %d rf.nextIndex[%d] : %d", rf.me, rpc.peerIndex, rf.nextIndex[rpc.peerIndex])
			go rf.sendRealAppendEntries(rpc.peerIndex, replyChan) // retry
		}
		rf.mu.Unlock() // should not hold the lock while reading from any channel, in this case replyChan
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	DPrintf("Raft # %d in function updateLeaderCommitIndex()", rf.me)
	rf.mu.Lock()
	matchIndex := append(make([]int, 0), rf.matchIndex...)
	rf.mu.Unlock()
	sort.Sort(sort.IntSlice(matchIndex))
	medianCommitIndex := matchIndex[rf.majorityNeed-1]
	rf.mu.Lock()
	if medianCommitIndex > rf.commitIndex && rf.log[medianCommitIndex].TermReceived == rf.currentTerm {
		rf.commitIndex = medianCommitIndex
	}
	rf.mu.Unlock()
}
