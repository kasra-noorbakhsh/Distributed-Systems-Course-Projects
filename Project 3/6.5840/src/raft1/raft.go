package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const SLEEP_TIME = 20 * time.Millisecond
const SLEEP_BETWEEN_APPEND_ENTRIES = 100 * time.Millisecond

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

	nextIndex  []int
	matchIndex []int

	commitIndex  int
	lastApplied  int
	timeout      *time.Timer
	state        State
	currentIndex int
	applyCh      chan raftapi.ApplyMsg
}

func (rf *Raft) setState(state_ State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state_
}

func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == LEADER
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = LEADER

	for i := range len(rf.peers) {
		// if len(rf.log) == 0 {
		// 	rf.nextIndex[i] = 0
		// } else {
		// 	rf.nextIndex[i] = len(rf.log) - 1
		// }
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) becomeFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = FOLLOWER
	rf.votedFor = -1
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

func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (rf *Raft) incrementLastApplied() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied++
}

func (rf *Raft) getPeers() []*labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers
}

func (rf *Raft) getMe() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.me
}

func (rf *Raft) getLog() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

func (rf *Raft) getLogSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

func (rf *Raft) truncateLog(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = rf.log[:index+1]
}

func (rf *Raft) appendLogEntries(entries ...LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < 0 || index >= len(rf.log) {
		return LogEntry{}
	}
	return rf.log[index]
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

func (rf *Raft) getNextIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[server]
}

func (rf *Raft) setNextIndex(server int, nextIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = nextIndex
}

func (rf *Raft) decrementNextIndex(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server]--
}

func (rf *Raft) getMatchIndex() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex
}

func (rf *Raft) setMatchIndex(server int, matchIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[server] = matchIndex
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

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = commitIndex
}

func (rf *Raft) getMajority() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majority := len(rf.peers)/2 + 1
	return majority
}

func (rf *Raft) getIndex(command interface{}) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i, entry := range rf.log {
		if entry.Command == command {
			return i
		}
	}
	return -1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	return rf.getCurrentTerm(), rf.isLeader()
}

func (rf *Raft) getTimeoutDuration() int64 {
	var ms int64
	if rf.isLeader() {
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
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	if rf.lastLogTerm() > args.LastLogTerm {
		return true
	}
	if rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
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
		rf.becomeFollower()
		if !rf.isMoreUpToDate(args) {
			rf.setVotedFor(args.CandidateId)
			reply.VoteGranted = true
			// fmt.Println(rf.getMe(), "Vote granted to", args.CandidateId, "term:", rf.getCurrentTerm())
		}
		return
	}
	if (rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateId) && !rf.isMoreUpToDate(args) {
		rf.setVotedFor(args.CandidateId)
		reply.VoteGranted = true
		// fmt.Println(rf.getMe(), "Vote granted to", args.CandidateId, "term:", rf.getCurrentTerm())
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

func (rf *Raft) sendAppendEntriesToFollower(server int, term int, replyCh chan AppendEntriesReply) {
	prevLogIndex := rf.getNextIndex(server) - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.getLogEntry(prevLogIndex).Term
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.getMe(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.log[rf.getNextIndex(server):],
		LeaderCommit: rf.getCommitIndex(),
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, &args, &reply)
	replyCh <- reply
}

func (rf *Raft) handleAppendEntriesReply(server int, replyCh chan AppendEntriesReply) {
	reply := <-replyCh
	if reply.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(reply.Term)
		rf.becomeFollower()
	}
	if reply.Success {
		rf.setNextIndex(server, reply.LastIndex+1)
		rf.setMatchIndex(server, reply.LastIndex)
	} else {
		if rf.getNextIndex(server) > 0 {
			// fmt.Println("Follower", server, "term:", rf.getCurrentTerm(), "rejected append entries")
			rf.decrementNextIndex(server)
		}
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	for i := rf.getCommitIndex() + 1; i < rf.getLogSize(); i++ {
		count := 0
		for _, matchIndex := range rf.getMatchIndex() {
			if matchIndex >= i && rf.getLogEntry(i).Term == rf.getLogEntry(matchIndex).Term {
				count++
			}
		}
		if count >= rf.getMajority() {
			rf.setCommitIndex(i)
			// fmt.Println("Leader updated commit index to", i, "Log:", rf.getLog())
		}
	}
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
	// Your code here (3B).
	term := rf.getCurrentTerm()
	if !rf.isLeader() {
		return -1, term, false
	}

	if command != nil {
		rf.appendLogEntries(LogEntry{
			Term:    term,
			Command: command,
		})
	}
	rf.setNextIndex(rf.getMe(), rf.getLogSize())
	rf.setMatchIndex(rf.getMe(), rf.getLogSize()-1)

	// fmt.Println("Leader", rf.getMe(), "term:", rf.getCurrentTerm(), "appending command:", command, "log:", rf.getLog())
	for i := range rf.getPeers() {
		if i == rf.getMe() {
			continue
		}
		go func(server int) {
			for rf.getNextIndex(server) < rf.getLogSize() && rf.isLeader() {
				replyCh := make(chan AppendEntriesReply, 1)

				go rf.sendAppendEntriesToFollower(server, term, replyCh)
				go rf.handleAppendEntriesReply(server, replyCh)

				time.Sleep(SLEEP_BETWEEN_APPEND_ENTRIES)
			}
		}(i)
	}
	time.Sleep(SLEEP_TIME)
	rf.updateLeaderCommitIndex()

	index := rf.getIndex(command) + 1
	return index, term, true
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
				Term:         rf.getCurrentTerm(),
				LeaderId:     rf.me,
				LeaderCommit: rf.getCommitIndex(),
			}
			reply := AppendEntriesReply{}
			// fmt.Println(rf.me, "sending heartbeat to", server, "term:", rf.getCurrentTerm())
			rf.sendAppendEntries(server, &args, &reply)
			if reply.Term > rf.getCurrentTerm() {
				// fmt.Println(rf.me, "became a follower")
				rf.setCurrentTerm(reply.Term)
				rf.becomeFollower()
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

		if rf.isLeader() {
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
			LastLogIndex: rf.lastLogIndex(),
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
					rf.becomeLeader()
					rf.Start(nil)
					go rf.sendHeartbeat()
					rf.resetTimer()
					// rf.clearVotedFor()
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
	rf.currentIndex = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 0)
	rf.startTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommitedEntry()

	return rf
}

func (rf *Raft) applyCommitedEntry() {
	for {
		for i := rf.getLastApplied() + 1; i <= rf.getCommitIndex(); i++ {
			entry := rf.getLogEntry(i)
			applyMessage := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: i + 1,
			}
			rf.incrementLastApplied()
			rf.applyCh <- applyMessage
		}
		time.Sleep(SLEEP_TIME)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.getCurrentTerm()
	reply.Id = rf.getMe()
	reply.Success = false

	if args.Term < rf.getCurrentTerm() {
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.becomeFollower()
	}
	// if rf.isLeader() && args.LeaderId != rf.me {
	// 	// fmt.Println(rf.me, "became a follower", "term:", rf.getCurrentTerm(), "leader:", args.LeaderId)
	// 	rf.setState(FOLLOWER)
	// }
	// rf.clearVotedFor()
	rf.resetTimer()

	if args.isHeartbeat() {
		rf.updateFollowerCommitIndex(args.LeaderCommit)
		return
	}

	if args.PrevLogIndex >= rf.getLogSize() {
		return
	}
	if args.PrevLogIndex >= 0 && rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		return
	}

	rf.truncateLog(args.PrevLogIndex)
	rf.appendLogEntries(args.Entries...)
	rf.updateFollowerCommitIndex(args.LeaderCommit)

	reply.LastIndex = args.PrevLogIndex + len(args.Entries)
	reply.Success = true
}

func (rf *Raft) updateFollowerCommitIndex(leaderCommit int) {
	if rf.isLeader() {
		return
	}
	if leaderCommit > rf.getCommitIndex() {
		lastNewIndex := rf.getLogSize() - 1
		rf.setCommitIndex(min(leaderCommit, lastNewIndex))
		// fmt.Println(rf.me, "updating commit index", "leader commit:", leaderCommit, "last new index:", lastNewIndex, "current commit index:", rf.getCommitIndex())
	}
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
	Id        int
	Term      int
	Success   bool
	LastIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (arg *AppendEntriesArgs) isHeartbeat() bool {
	return len(arg.Entries) == 0
}
