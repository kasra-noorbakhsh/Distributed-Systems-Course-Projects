package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	timeout     *time.Timer
	isLeader    bool
}

func (rf *Raft) getIsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isLeader
}

func (rf *Raft) setIsLeader(isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isLeader = isLeader
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) setVotedFor(votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = votedFor
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	return rf.getCurrentTerm(), rf.getIsLeader()
}

func (rf *Raft) startTimer() {
	if rf.timeout != nil {
		rf.timeout.Stop()
	}
	var ms int64
	if rf.getIsLeader() {
		ms = 100
	} else {
		ms = 150 + (rand.Int63() % 150)
	}
	rf.timeout = time.NewTimer(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) resetTimer() {
	var ms int64
	if rf.getIsLeader() {
		ms = 100
	} else {
		ms = 150 + (rand.Int63() % 150)
	}
	rf.timeout.Reset(time.Duration(ms) * time.Millisecond)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) lastLogTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex == -1 {
		return -1
	}
	return rf.log[rf.commitIndex].term
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	if rf.lastLogTerm() > args.LastLogTerm {
		return true
	}
	if rf.lastLogTerm() == args.LastLogTerm && rf.commitIndex > args.LastLogIndex {
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println(rf.me, "received RequestVote from", args.CandidateId, "term:", args.Term, "current term:", rf.getCurrentTerm(), "votedFor:",  rf.getVotedFor())
	// Your code here (3A, 3B).
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if (rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateId) && !rf.isMoreUpToDate(args) {
		rf.setVotedFor(args.CandidateId)
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeat() {
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			args := AppendEntriesArgs{
				Term:     rf.getCurrentTerm(),
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			// fmt.Println(rf.me, "sending heartbeat to", server, "term:", rf.getCurrentTerm())
			rf.sendAppendEntries(server, &args, &reply)
			if reply.Term > rf.getCurrentTerm() {
				fmt.Println(rf.me, "became a follower")
				rf.setCurrentTerm(reply.Term)
				rf.setIsLeader(false)
				rf.setVotedFor(-1)
				rf.resetTimer()
			}
		}(i)
	}
	wg.Wait()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		<-rf.timeout.C
		rf.resetTimer()

		if rf.getIsLeader() {
			// fmt.Println(rf.me, "is leader, sending heartbeat")
			go rf.sendHeartbeat()
			continue
		}
		fmt.Println(rf.me, "starting election term:", rf.getCurrentTerm())
		rf.setCurrentTerm(rf.getCurrentTerm() + 1)
		rf.setVotedFor(rf.me)

		votes := 1
		voteCh := make(chan bool, len(rf.peers)-1)
		termCh := make(chan int, len(rf.peers)-1)

		args := RequestVoteArgs{
			Term:         rf.getCurrentTerm(),
			CandidateId:  rf.me,
			LastLogIndex: rf.commitIndex,
			LastLogTerm:  rf.lastLogTerm(),
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(peer, &args, &reply)
				termCh <- reply.Term
				voteCh <- reply.VoteGranted
			}(i)
		}

		majority := len(rf.peers)/2 + 1
		received := 1

		go func() {
			for received < len(rf.peers) {
				select {
				case granted := <-voteCh:
					received++
					if granted {
						votes++
					}
					if votes >= majority {
						fmt.Println(rf.me, "became leader term:", rf.getCurrentTerm())
						rf.setIsLeader(true)
						rf.setVotedFor(-1)
						go rf.sendHeartbeat()
						return
					}
				case newTerm := <-termCh:
					if newTerm > rf.getCurrentTerm() {
						rf.setCurrentTerm(newTerm)
						rf.setVotedFor(-1)
						rf.resetTimer()
						return
					}
				}
			}
			rf.setVotedFor(-1)

		}()
		// rf.setVotedFor(-1)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.log = make([]LogEntry, 0)
	rf.startTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println(rf.me, "received heartbeat from", args.LeaderId, "term:", args.Term, "current term", rf.currentTerm)
	reply.Term = rf.getCurrentTerm()
	if args.Term < rf.getCurrentTerm() {
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
	}
	if rf.getIsLeader() && args.LeaderId != rf.me {
		fmt.Println(rf.me, "became a follower", "term:", rf.getCurrentTerm(), "leader:", args.LeaderId)
		rf.setIsLeader(false)
	}
	rf.setVotedFor(-1)
	rf.resetTimer()
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
