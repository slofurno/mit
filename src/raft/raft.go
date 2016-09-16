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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term  int
	Entry string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	ticker *time.Ticker

	isLeader bool

	votes   int
	timeout int
	ting    chan struct{}
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	fmt.Println("id:", rf.me, "term:", rf.currentTerm, "isleader", rf.isLeader)
	return rf.currentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CanidateId   int
	LastLogIndex int
	LastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor >= 0 && rf.votedFor != args.CanidateId {
		reply.VoteGranted = false
		return
	}

	index, term := rf.lastLogIndexAndTerm()
	if term > args.LastLogTerm || index > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CanidateId
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//this was resetting votedfor....
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.votedFor = -1
	}

	rf.timeout = 15 + rand.Intn(15)
	return
}

func (rf *Raft) heartbeat() {
	index, term := rf.lastLogIndexAndTerm()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: index,
		PrevLogTerm:  term,
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex,
	}

	for i, p := range rf.peers {
		if i != rf.me {
			go func(peer *labrpc.ClientEnd, args AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				peer.Call("Raft.AppendEntries", args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.isLeader = false
				}
			}(p, args)
		}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	lastTerm := 0
	lastIndex := len(rf.log)

	if lastIndex > 0 {
		lastTerm = rf.log[lastIndex-1].Term
	}

	return lastIndex, lastTerm
}

func (rf *Raft) requestVotes() {
	rf.currentTerm++
	rf.votedFor = rf.me
	initialTerm := rf.currentTerm
	votes := 1

	fmt.Println(rf.me, "holding an election")
	index, term := rf.lastLogIndexAndTerm()

	args := RequestVoteArgs{
		Term:         initialTerm,
		CanidateId:   rf.me,
		LastLogIndex: index,
		LastLogTerm:  term,
	}

	for i, p := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer *labrpc.ClientEnd) {
			reply := &RequestVoteReply{}
			peer.Call("Raft.RequestVote", args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm > initialTerm {
				fmt.Println("out of date vote result")
				return
			}

			if reply.Term > rf.currentTerm {
				fmt.Println("voter is next term")
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.isLeader = false
				return
			}

			if reply.VoteGranted {
				votes++
				fmt.Printf("up to %d votes\n", votes)
			} else {
				fmt.Println("vote not granted")
			}

			majority := len(rf.peers) / 2
			if votes > majority {
				fmt.Println(rf.me, "won the election")
				rf.isLeader = true
			}

		}(p)
	}
}

func (rf *Raft) Tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader {
		rf.heartbeat()
		return
	}

	rf.timeout--

	//fmt.Println(rf.me, rf.timeout)
	if rf.timeout == 0 {
		rf.timeout = 15 + rand.Intn(15)
		rf.requestVotes()
		//become candidate
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.timeout = 15 + rand.Intn(15)
	rf.votedFor = -1

	go func() {
		for {
			rf.Tick()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
