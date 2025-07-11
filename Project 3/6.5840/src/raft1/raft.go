package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	mu2       sync.Mutex          // Lock to protect shared access to this peer's state
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// These 3 below are Persistent state fields
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

func (rf *Raft) getPeers() []*labrpc.ClientEnd {
	return rf.peers
}

func (rf *Raft) getPeer(server int) *labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers[server]
}

func (rf *Raft) truncateLog(index int) {
	if rf.state == LEADER {
		fmt.Println("truncate leader's log")
	}
	rf.log = rf.log[:index+1]
}

func (rf *Raft) appendLogEntries(entries ...LogEntry) {
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) decrementNextIndex(server int) {
	rf.nextIndex[server]--
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

func (rf *Raft) getCurrentTerm() int {
	return rf.currentTerm
}

func (rf *Raft) getMe() int {
	return rf.me
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.currentTerm = term
}

func (rf *Raft) becomeFollower() {
	rf.state = FOLLOWER
	rf.votedFor = -1
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index < 0 || index >= len(rf.log) {
		return LogEntry{}
	}
	return rf.log[index]
}

func (rf *Raft) getLogSize() int {
	return len(rf.log)
}

func (rf *Raft) resetTimer() {
	ms := rf.getTimeoutDuration()
	rf.timeout.Reset(time.Duration(ms) * time.Millisecond)
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

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) setVotedFor(votedFor int) {
	rf.votedFor = votedFor
}

func (rf *Raft) getVotedFor() int {
	return rf.votedFor
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

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLogSuffix(startIndex int) []LogEntry {
	entries := make([]LogEntry, len(rf.log[startIndex:]))
	copy(entries, rf.log[startIndex:])
	return entries
}

func (rf *Raft) setNextIndex(server int, nextIndex int) {
	rf.nextIndex[server] = nextIndex
}

func (rf *Raft) setMatchIndex(server int, matchIndex int) {
	rf.matchIndex[server] = matchIndex
}

func (rf *Raft) getNextIndex(server int) int {
	return rf.nextIndex[server]
}

func (rf *Raft) lastCommitTerm() int {
	if rf.commitIndex < 0 || rf.commitIndex >= len(rf.log) {
		return -1
	}
	return rf.log[rf.commitIndex].Term
}

func (rf *Raft) incrementTerm() {
	rf.currentTerm++
}

func (rf *Raft) voteForSelf() {
	rf.votedFor = rf.me
}

func (rf *Raft) clearVotedFor() {
	rf.votedFor = -1
}

func (rf *Raft) becomeLeader() {
	rf.state = LEADER

	for i := range len(rf.peers) {
		// if len(rf.log) == 0 {
		// 	rf.nextIndex[i] = 0
		// } else {
		// 	rf.nextIndex[i] = len(rf.log) - 1
		// }
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) getMajority() int {
	majority := len(rf.peers)/2 + 1
	return majority
}

func (rf *Raft) setLog(log []LogEntry) {
	rf.log = log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getCurrentTerm(), rf.isLeader()
}

func (rf *Raft) startTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.timeout != nil {
		rf.timeout.Stop()
	}
	ms := rf.getTimeoutDuration()
	rf.timeout = time.NewTimer(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) waitForTimeout() {
	<-rf.timeout.C
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// Encode persistent state
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil) // no snapshot yet
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Failed to decode persisted Raft state")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.setCurrentTerm(currentTerm)
		rf.setVotedFor(votedFor)
		rf.setLog(log)
	}
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.getCurrentTerm()

	if args.Term < rf.getCurrentTerm() {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		reply.Term = rf.getCurrentTerm()
		rf.becomeFollower()
		if !rf.isMoreUpToDate(args) {
			rf.setVotedFor(args.CandidateId)
			reply.VoteGranted = true
			// fmt.Println(rf.getMe(), "Vote granted to", args.CandidateId, "term:", rf.getCurrentTerm())
		}
		rf.persist()
		return
	}
	if (rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateId) && !rf.isMoreUpToDate(args) {
		rf.setVotedFor(args.CandidateId)
		rf.persist()
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

func (rf *Raft) sendAppendEntriesToFollower(server int, term int, replyCh chan AppendEntriesReply, okCh chan bool) {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.getLogSuffix(prevLogIndex + 1),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	okCh <- ok
	if ok {
		replyCh <- reply
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, replyCh chan AppendEntriesReply, okCh chan bool) {
	ok := <-okCh
	if !ok {
		return
	}
	reply := <-replyCh

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(reply.Term)
		rf.becomeFollower()
		rf.persist()
	}
	if reply.Success {
		rf.setNextIndex(server, reply.LastIndex+1)
		rf.setMatchIndex(server, reply.LastIndex)
	} else {
		if rf.nextIndex[server] > 0 {
			rf.decrementNextIndex(server)
		}
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}

	currentTerm := rf.currentTerm
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		count := 0
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= i && rf.log[i].Term == currentTerm {
				count++
			}
		}
		majority := len(rf.peers)/2 + 1
		if count >= majority {
			rf.commitIndex = i
			rf.applyCommitedEntry()
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
	rf.mu.Lock()
	term := rf.getCurrentTerm()
	if !rf.isLeader() {
		rf.mu.Unlock()
		return -1, term, false
	}

	if command != nil {
		rf.appendLogEntries(LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
	}
	rf.setNextIndex(rf.getMe(), rf.getLogSize())
	rf.setMatchIndex(rf.getMe(), rf.getLogSize()-1)
	rf.mu.Unlock()

	// fmt.Println("Leader", rf.getMe(), "term:", rf.getCurrentTerm(), "appending command:", command, "log:", rf.getLog())
	for i := range rf.getPeers() {
		if i == rf.getMe() {
			continue
		}
		go func(server int) {
			for rf.shouldSendEntryToFollower(server) {
				replyCh := make(chan AppendEntriesReply, 1)
				okCh := make(chan bool, 1)

				go rf.sendAppendEntriesToFollower(server, term, replyCh, okCh)
				go rf.handleAppendEntriesReply(server, replyCh, okCh)

				time.Sleep(SLEEP_BETWEEN_APPEND_ENTRIES)
			}
		}(i)
	}
	time.Sleep(SLEEP_TIME)
	rf.updateLeaderCommitIndex()

	index := rf.getIndex(command) + 1
	return index, term, true
}

func (rf *Raft) shouldSendEntryToFollower(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getNextIndex(server) < rf.getLogSize() && rf.isLeader() && !rf.killed()
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

func (rf *Raft) sendHeartbeatToPeers() {
	for i := range rf.getPeers() {
		if i == rf.getMe() {
			continue
		}
		go func(server int) {
			reply := rf.sendHeartbeatTo(server)
			rf.handleHeartbeatReply(reply)
		}(i)
	}
}

func (rf *Raft) sendHeartbeatTo(server int) AppendEntriesReply {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:           rf.currentTerm,
		LeaderId:       rf.me,
		LeaderCommit:   rf.commitIndex,
		LastCommitTerm: rf.lastCommitTerm(),
		IsHeartbeat:    true,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	rf.sendAppendEntries(server, &args, &reply)
	return reply
}

func (rf *Raft) handleHeartbeatReply(reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(reply.Term)
		rf.becomeFollower()
		rf.resetTimer()
		rf.persist()
	}
}

func (rf *Raft) sendRequestVoteToPeers(replies chan RequestVoteReply) {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.getMe(),
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.mu.Unlock()

	for i := range rf.getPeers() {
		if i == rf.getMe() {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(peer, &args, &reply)
			replies <- reply
		}(i)
	}
}

func (rf *Raft) handleRequestVoteReplies(replies chan RequestVoteReply) {
	votes := 1
	received := 1

	for received < len(rf.peers) {
		reply := <-replies
		received++

		rf.mu.Lock()
		if reply.Term > rf.getCurrentTerm() {
			rf.setCurrentTerm(reply.Term)
			rf.clearVotedFor()
			rf.resetTimer()
			rf.persist()

			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted && reply.Term == rf.getCurrentTerm() {
			votes++
		}
		if votes >= rf.getMajority() {
			// fmt.Println(rf.getMe(), "became leader term:", rf.getCurrentTerm())
			rf.becomeLeader()
			rf.resetTimer()

			go rf.Start(nil)
			go rf.sendHeartbeatToPeers()

			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	rf.clearVotedFor()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		rf.waitForTimeout()
		rf.mu.Lock()

		rf.resetTimer()
		if rf.isLeader() {
			go rf.sendHeartbeatToPeers()
			rf.mu.Unlock()
			continue
		}
		// fmt.Println(rf.getMe(), "starting election term:", rf.getCurrentTerm())
		rf.incrementTerm()
		rf.voteForSelf()
		rf.persist()

		replies := make(chan RequestVoteReply, len(rf.peers)-1)

		go rf.sendRequestVoteToPeers(replies)
		go rf.handleRequestVoteReplies(replies)

		rf.mu.Unlock()
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
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}
	rf.nextIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 0)
	rf.startTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Clamp commitIndex to last log index after restore
	if rf.commitIndex > len(rf.log)-1 {
		rf.commitIndex = len(rf.log) - 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) applyCommitedEntry() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		entry := LogEntry{}
		if i < len(rf.log) {
			entry = rf.log[i]

		}
		applyMessage := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: i + 1,
		}
		if rf.killed() {
			return
		}
		if applyMessage.Command == nil {
			fmt.Println("Entry:", entry, "lastApplied:", rf.lastApplied, "commitIndex:", rf.commitIndex, "log size:", len(rf.log))
			continue
		}
		rf.applyCh <- applyMessage
		rf.lastApplied++
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.getCurrentTerm()
	reply.Id = rf.getMe()
	reply.Success = false

	if args.Term < rf.getCurrentTerm() {
		return
	}
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.becomeFollower()
		rf.persist()
	}
	rf.resetTimer()

	if args.IsHeartbeat {
		if args.LastCommitTerm == rf.getLogEntry(args.LeaderCommit).Term {
			rf.updateFollowerCommitIndex(args.LeaderCommit)
		}
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
	rf.persist()

	reply.LastIndex = args.PrevLogIndex + len(args.Entries)
	reply.Success = true
}

func (rf *Raft) updateFollowerCommitIndex(leaderCommit int) {
	if rf.state == LEADER {
		return
	}
	if leaderCommit > rf.commitIndex {
		lastNewIndex := len(rf.log) - 1
		rf.commitIndex = min(leaderCommit, lastNewIndex)
		rf.applyCommitedEntry()
	}
}

type AppendEntriesArgs struct {
	Term           int
	LeaderId       int
	PrevLogIndex   int
	PrevLogTerm    int
	Entries        []LogEntry
	LeaderCommit   int
	IsHeartbeat    bool
	LastCommitTerm int
}

type AppendEntriesReply struct {
	Id        int
	Term      int
	Success   bool
	LastIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.getPeer(server).Call("Raft.AppendEntries", args, reply)
	return ok
}
