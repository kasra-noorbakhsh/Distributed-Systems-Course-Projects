package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	// "fmt"

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
	Term    int
	Command interface{}
}

type State int

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

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
	state       State
	applyCh     chan raftapi.ApplyMsg
}

func (rf *Raft) setState(state_ State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state_
}

func (rf *Raft) getState() State {
	rf.mu.Lock()
	rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) getIsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == LEADER
}

// func (rf *Raft) setIsLeader(isLeader bool) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if isLeader {
// 		rf.state = LEADER
// 	} else {
// 		rf.state = FOLLOWER
// 	}
// }

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

func (rf *Raft) incrementTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
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

func (rf *Raft) voteForSelf() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = rf.me
}

func (rf *Raft) clearVotedFor() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = -1
}

func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) getMajority() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majority := len(rf.peers)/2 + 1
	return majority
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	return rf.getCurrentTerm(), rf.getIsLeader()
}

func (rf *Raft) getTimeoutDuration() int64 {
	var ms int64
	if rf.getIsLeader() {
		ms = 100
	} else {
		ms = 150 + (rand.Int63() % 150)
	}
	return ms
}

func (rf *Raft) startTimer() {
	if rf.timeout != nil {
		rf.timeout.Stop()
	}
	ms := rf.getTimeoutDuration()
	rf.timeout = time.NewTimer(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) resetTimer() {
	ms := rf.getTimeoutDuration()
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
	return rf.log[rf.commitIndex].Term
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	if rf.lastLogTerm() > args.LastLogTerm {
		return true
	}
	if rf.lastLogTerm() == args.LastLogTerm && rf.getCommitIndex() > args.LastLogIndex {
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// fmt.Println(rf.me, "received RequestVote from", args.CandidateId, "term:", args.Term, "current term:", rf.getCurrentTerm(), "votedFor:",  rf.getVotedFor())
	// Your code here (3A, 3B).
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setState(FOLLOWER)
		rf.setVotedFor(args.CandidateId)
		reply.VoteGranted = true
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
	index := len(rf.log)
	term := rf.getCurrentTerm()
	prevLogIndex := index - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	isLeader := rf.getIsLeader()
	// Your code here (3B).
	if isLeader {
		for i := range rf.peers {
			if i == rf.me {
				continue
			} else {
				go func(server int) {
					log := LogEntry{
						Term:    term,
						Command: command,
					}
					args := AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      []LogEntry{log},
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, &args, &reply)
				}(i)
			}
		}
	}

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
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := AppendEntriesArgs{
				Term:     rf.getCurrentTerm(),
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			// fmt.Println(rf.me, "sending heartbeat to", server, "term:", rf.getCurrentTerm())
			rf.sendAppendEntries(server, &args, &reply)
			if reply.Term > rf.getCurrentTerm() {
				// fmt.Println(rf.me, "became a follower")
				rf.setCurrentTerm(reply.Term)
				rf.setState(FOLLOWER)
				rf.clearVotedFor()
				rf.resetTimer()
			}
		}(i)
	}
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
		// fmt.Println(rf.me, "starting election term:", rf.getCurrentTerm())
		rf.incrementTerm()
		rf.voteForSelf()

		args := RequestVoteArgs{
			Term:         rf.getCurrentTerm(),
			CandidateId:  rf.me,
			LastLogIndex: rf.getCommitIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}

		votes := 1
		received := 1
		majority := rf.getMajority()
		replies := make(chan RequestVoteReply, len(rf.peers)-1)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(peer, &args, &reply)
				replies <- reply
			}(i)
		}

		go func() {
			for received < len(rf.peers) {
				reply := <-replies
				received++
				if reply.Term > rf.getCurrentTerm() {
					rf.setCurrentTerm(reply.Term)
					rf.clearVotedFor()
					rf.resetTimer()
					return
				}
				if reply.VoteGranted {
					votes++
				}
				if votes >= majority {
					// fmt.Println(rf.me, "became leader term:", rf.getCurrentTerm())
					rf.setState(LEADER)
					go rf.sendHeartbeat()
					rf.resetTimer()
					rf.clearVotedFor()
					return
				}
			}
			rf.clearVotedFor()
		}()
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
	rf.applyCh = applyCh
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

// func (rf *Raft) ApplyCommitedEntry(entry LogEntry){
// 	applyMessage := raftapi.ApplyMsg{
// 		CommandValid : true,
// 		Command : entry.Command,
// 		//TODO:Add command index to logEntry
// 	}
// }

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.setCurrentTerm(args.Term)
		rf.clearVotedFor()
		rf.setState(FOLLOWER)
	}

	rf.resetTimer()

	if args.PrevLogIndex >= len(rf.log) {
		return
	}
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	index := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if index+i < len(rf.log) {
			if rf.log[index+i].Term != entry.Term {
				rf.log = rf.log[:index+i]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
