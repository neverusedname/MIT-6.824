package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	crand "crypto/rand"
	"log"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int

	rand             *rand.Rand
	minTimeoutMillis int
	maxTimeoutMillis int
	timeoutAtNanos   int64 // unix millis

	role   int32 // 0: follower, 1, candidate, 2 leader
	quorum int

	// 2B code goes here
	log         []LogEntry
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	logAck map[int]int // logIndex -> how many acks

	nexIndex   []int
	matchIndex []int

	applyCh chan ApplyMsg

	broadcastLogChan []chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = rf.role == 2

	return term, isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogTerm  int
	PrevLogIndex int
	Entries      []interface{}

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("%v got request vote from %v, candidate term: %v\n", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // candidateTerm < currentTerm, reject
		log.Printf("%v rejected request vote from %v.\n", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// election restriction, compare by term
	lastEntry := rf.log[len(rf.log)-1] // [index, term, cmd]
	if args.LastLogTerm < lastEntry.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// election restriction, compare by log index
	if args.LastLogIndex < lastEntry.Index {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm { // candidate term > currentTerm, grant
		log.Println(rf.me, "granted request vote from", args.CandidateId)
		rf.currentTerm = args.Term

		// there should be a process of immediately revert to follower state, simple skip

		rf.role = 0
		rf.votedFor = args.CandidateId
		rf.resetTimeout()

		reply.VoteGranted = true
		reply.Term = rf.currentTerm

		return
	}

	// now the term are equal
	reply.Term = rf.currentTerm
	if rf.votedFor < 0 { // haven't voted for no body
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		log.Println(rf.me, "granted request vote from", args.CandidateId)
	} else if rf.votedFor == args.CandidateId { // this should never happen
		reply.VoteGranted = true
	} else {
		log.Println(rf.me, "rejected req vote from", args.CandidateId, ", co's it has voted for", rf.votedFor)
		reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("%v got client cmd: %v\n", rf.me, command)

	if rf.role != 2 { // not leader
		return -1, -1, false
	}

	// Your code here (2B).
	logIndex := rf.nexIndex[rf.me]
	entry := LogEntry{rf.currentTerm, logIndex, command}
	rf.log = append(rf.log, entry)

	rf.logAck[logIndex] = 1
	rf.nexIndex[rf.me]++

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me { // don't send to myself
			continue
		}

		go func(serverIdx int) { // let broadcast routine broad cast
			rf.broadcastLogChan[serverIdx] <- logIndex
			//log.Printf("%v scheduled to broadcast log entry %v to %v", rf.me, entry, serverIdx)
		}(i)
	}

	return logIndex, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	log.Println(rf.me, " start ticker.")
	rf.mu.Lock()
	rf.resetTimeout()
	rf.mu.Unlock()

	for rf.killed() == false {
		rf.mu.Lock()
		span := rf.timeoutAtNanos - time.Now().UnixNano()
		rf.mu.Unlock()

		if span > 0 {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			time.Sleep(time.Nanosecond * time.Duration(span))
		}

		rf.mu.Lock()
		shouldTimeout := time.Now().UnixNano() > rf.timeoutAtNanos
		rf.mu.Unlock()

		if shouldTimeout {
			rf.competeForLeader()
		}
	}
}

func (rf *Raft) competeForLeader() {
	log.Println(rf.me, "timeout, running for leader...")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// vote for myself
	rf.role = 1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetTimeout()

	var votes int32 = 1

	for idx, peer := range rf.peers {
		if idx == rf.me { // skip myself
			continue
		}

		go func(peer *labrpc.ClientEnd, i int) {
			req := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}

			lastLogEntry := rf.log[len(rf.log)-1]
			req.LastLogTerm = lastLogEntry.Term // correct
			req.LastLogIndex = lastLogEntry.Index

			reply := RequestVoteReply{}

			log.Println(rf.me, "sending request vote to", i)
			ok := peer.Call("Raft.RequestVote", &req, &reply)

			if ok && reply.VoteGranted {
				log.Println(rf.me, "got vote granted from ", i, ".")
				voteCount := atomic.AddInt32(&votes, 1)

				if voteCount == int32(rf.quorum) {
					go rf.takeOver()
				}

			}

		}(peer, idx)
	}
}

func (rf *Raft) resetTimeout() {
	spanMillis := rf.rand.Intn(rf.maxTimeoutMillis-rf.minTimeoutMillis) + rf.minTimeoutMillis
	rf.timeoutAtNanos = time.Now().UnixNano() + int64(spanMillis*1000000)
}

func (rf *Raft) takeOver() {
	log.Println(rf.me, "takes over.")

	atomic.StoreInt32(&rf.role, 2)
	rf.startBroadcasters()

	for atomic.LoadInt32(&rf.role) == 2 { // as long as I am master

		rf.mu.Lock()
		rf.resetTimeout() // reset timeout of myself, send append entries to myself
		rf.mu.Unlock()

		// send periodic heartbeats to suppress peer timeouts
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(peer *labrpc.ClientEnd) {
				args := AppendEntriesArgs{}

				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex

				reply := AppendEntriesReply{}

				peer.Call("Raft.AppendEntries", &args, &reply)

				if !reply.Success && reply.Term > rf.currentTerm {
					// immediately reverts to follower
					rf.mu.Lock()
					rf.revertToFollower(reply.Term)
					rf.mu.Unlock()
				}

			}(rf.peers[i])
		}

		span := time.Duration(float64(rf.minTimeoutMillis) * 0.75)
		time.Sleep(time.Millisecond * span)
	}
}

func (rf *Raft) startBroadcasters() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		timeout := time.Duration(float64(rf.minTimeoutMillis) * 0.5)

		go func(serverIdx int) {
			log.Printf("%v starting broadcaster to %v\n", rf.me, serverIdx)
			ch := rf.broadcastLogChan[serverIdx]
			for atomic.LoadInt32(&rf.role) == 2 { // as long as I am still leader
				select {
				case logIndex := <-ch:
					rf.doBroadCastLogToFollower(serverIdx, logIndex)
				case <-time.After(time.Millisecond * timeout):
					// just don't block on <- ch
				}
			}
			log.Printf("%v said wtf!", rf.me)
		}(i)
	}
}

func (rf *Raft) doBroadCastLogToFollower(serverIdx int, logIdx int) {

	if logIdx < rf.nexIndex[serverIdx] || logIdx <= rf.matchIndex[serverIdx] {
		log.Printf("Log entry %v already broadcased to follower %v\n", logIdx, serverIdx)
		return
	}

	if logIdx > rf.nexIndex[serverIdx] {
		log.Printf("Follower %v has not caught up with entry %v yet!\n", serverIdx, logIdx)
		return
	}

	log.Printf("%v do broadcasting entry %v to %v\n", rf.me, logIdx, serverIdx)
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	prevEntry := rf.log[logIdx-1]

	args.PrevLogIndex = prevEntry.Index
	args.PrevLogTerm = prevEntry.Term
	args.Entries = []interface{}{rf.log[logIdx]}
	args.LeaderCommit = rf.commitIndex

	reply := &AppendEntriesReply{}

	for {
		ok := rf.peers[serverIdx].Call("Raft.AppendEntries", args, reply)
		log.Printf("%v got broadcasting entry res from %v, res: %v", rf.me, serverIdx, reply)
		if !ok {
			time.Sleep(time.Duration(float64(rf.minTimeoutMillis)*0.5) * time.Millisecond)
			continue // retry
		}

		if reply.Success {
			rf.mu.Lock()
			rf.nexIndex[serverIdx] = logIdx + 1

			if logIdx > rf.commitIndex {
				n := rf.logAck[logIdx]
				n += 1
				rf.logAck[logIdx] = n

				if n == rf.quorum {

					rf.commitIndex = logIdx
					rf.matchIndex[serverIdx] = logIdx
					rf.matchIndex[rf.me] = logIdx

					log.Printf("%v committed log entry %v", rf.me, logIdx)

					delete(rf.logAck, logIdx)

					// Apply this message
					msg := ApplyMsg{}

					entry := rf.log[logIdx]
					msg.CommandIndex = logIdx
					msg.CommandValid = true
					msg.Command = entry.Command

					go func() { rf.applyCh <- msg }() // non blocking

					rf.lastApplied = logIdx
					log.Printf("%v applied log entry %v", rf.me, logIdx)
				}
			} else { // leader already commit this log
				rf.matchIndex[serverIdx] = logIdx
			}

			if len(rf.log) > logIdx+1 { // still got more entries to sync
				go func() { rf.broadcastLogChan[serverIdx] <- logIdx + 1 }() // non-blocking schedule to sync next entry
			}
			rf.mu.Unlock()
		} else { // reply not success
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.revertToFollower(reply.Term)
			} else { // just append entries failure
				rf.nexIndex[serverIdx]--
			}
			rf.mu.Unlock()
		}

		break
	}
}

func (rf *Raft) revertToFollower(term int) {
	rf.currentTerm = term
	rf.role = 0
	rf.resetTimeout()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("%v got append entries from %v\n", rf.me, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term {

		// heart beat logic
		rf.votedFor = -1 // Didn't vote i this term
		rf.revertToFollower(args.Term)
		reply.Term = rf.currentTerm
		reply.Success = true

		// real append entries logic
		rf.tryAppendEntries(args, reply)
		return
	}

	if rf.currentTerm == args.Term {
		if rf.me == args.LeaderId {
			// this should never happen
		} else { // term equal, and heart beat send not from self
			// heart beat logic
			rf.revertToFollower(args.Term)
			reply.Success = true
			reply.Term = rf.currentTerm

			// real append entries logic
			rf.tryAppendEntries(args, reply)
		}
		return
	}

	// rf.currentTerm > args.term
	reply.Term = rf.currentTerm
	reply.Success = false
}

func (rf *Raft) tryAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if len(args.Entries) > 0 {
		if rf.log[len(rf.log)-1].Index < args.PrevLogIndex { // ain't catchup yet
			// TODO: wrong logic
			log.Printf("%v not caught up with append entry req: %v\n", rf.me, args)
			reply.Success = false
			return
		}

		if rf.log[len(rf.log)-1].Index > args.PrevLogIndex { // this log already appended
			log.Printf("%v already processed duplicated entry req: %v\n", rf.me, args)
			reply.Success = true
			return
		}

		// only append when rf.log[len(rf.log)-1].Index == args.PrevLogIndex

		entry := args.Entries[0].(LogEntry)
		log.Printf("Folower %v append %v to its logs", rf.me, entry)
		rf.log = append(rf.log, entry)
	}

	for rf.commitIndex <= args.LeaderCommit {
		log.Printf("Folower %v commit entries with index %v", rf.me, rf.commitIndex)
		if rf.commitIndex > 0 {
			applyMsg := ApplyMsg{}

			commitEntry := rf.log[rf.commitIndex]
			applyMsg.Command = commitEntry.Command
			applyMsg.CommandIndex = commitEntry.Index
			applyMsg.CommandValid = true

			// apply to state machine
			go func() { rf.applyCh <- applyMsg }()
		}

		rf.lastApplied = rf.commitIndex
		rf.commitIndex += 1
		log.Printf("Follower %v commited and applied to %v", rf.me, rf.lastApplied)
	}

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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	labgob.Register(LogEntry{})

	log.Default().Println("Making raft, me: ", me)

	rf := &Raft{}
	rf.peers = peers
	rf.quorum = len(peers)/2 + 1
	rf.votedFor = -1
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	max := big.NewInt(int64(1) << 62)
	bigX, _ := crand.Int(crand.Reader, max)
	seed := bigX.Int64()
	rf.rand = rand.New(rand.NewSource(seed))
	rf.minTimeoutMillis = 100
	rf.maxTimeoutMillis = 300

	rf.votedFor = -1
	rf.currentTerm = 0

	rf.log = make([]LogEntry, 0, 256)
	rf.log = append(rf.log, LogEntry{0, 0, nil}) // put a dummy head

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.matchIndex = []int{0, 0, 0}
	rf.nexIndex = []int{1, 1, 1}

	// log replication related code goes here
	rf.applyCh = applyCh

	// initializing broadcasting channel
	rf.broadcastLogChan = make([]chan int, 0, 3)
	for i := 0; i < len(peers); i++ {
		rf.broadcastLogChan = append(rf.broadcastLogChan, make(chan int, 4))
	}

	rf.logAck = make(map[int]int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
