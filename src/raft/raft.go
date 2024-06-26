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
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Term int
type RoleID int
type Role int

const (
	Follower Role = iota + 1
	Candidate
	Leader
)

var RoleMap = map[Role]string{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role         Role
	currentTerm  Term
	votes        int
	votedFor     RoleID
	heartbeatCh  chan bool
	voteResultCh chan RequestVoteReply

	heartbeatInterval time.Duration
	electTimeout      time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = int(rf.currentTerm)
	var isleader = (rf.role == Leader)
	// Your code here (2A).
	LOGPRINT(INFO, dTrace, "No.%v return %v %v %v\n", rf.me, term, isleader, rf.role)
	return term, isleader
}

func (rf *Raft) VoteClear(term Term) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votes = 0
	rf.votedFor = -1
}

func (rf *Raft) VoteChClear() {
	for {
		select {
		case <-rf.voteResultCh:
		default:
			LOGPRINT(INFO, dVote, "No.%v ClearUseless Votes.\n")
			return
		}
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm Term
	CandidateId   RoleID

	LastLogIndex int
	LastLogTerm  Term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteTerm    Term
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CandidateTerm > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
	}

	reply.VoteTerm = rf.currentTerm

	// 如果当前节点还没有投票，并且候选人的任期不小于当前节点的任期，则投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.CandidateTerm >= rf.currentTerm {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		LOGPRINT(DEBUG, dVote, "No.%v args.CandidateTerm = %v, rf.currentTerm = %v\n", rf.me, args.CandidateTerm, rf.currentTerm)
		LOGPRINT(DEBUG, dVote, "No.%v(CurrentTerm=%v) agree to vote for No.%v\n", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		reply.VoteGranted = false
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			LOGPRINT(DEBUG, dVote, "No.%d(CurrentTerm=%v) refused to vote for %d, already voted for %d\n", rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)
		}
	}
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	LeaderTerm   Term
	LeaderID     RoleID
	PrevLogIndex uint64
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	CurrentTerm Term
	Success     bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

type LogEntry struct {
	CurrentTerm Term
	Command     interface{}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.LeaderTerm {
		// 当前节点的人任期更大时，拒绝收到的请求
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		return
	}

	//更新当前节点信息
	rf.currentTerm = args.LeaderTerm
	rf.votedFor = args.LeaderID
	rf.role = Follower

	//日志更新

	reply.CurrentTerm = rf.currentTerm
	reply.Success = true

	//reset heartsbeats
	LOGPRINT(INFO, dClient, "No.%v receive heartbeat from No.%v", rf.me, args.LeaderID)
	select {
	case rf.heartbeatCh <- true:
	default:
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	// Your code here (2B).

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
	fmt.Print("current call kill\n")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runAsFollower() {
	LOGPRINT(DEBUG, dClient, "No.%v runAsFollower\n", rf.me)
	timeout := rf.heartbeatInterval + time.Duration(rand.Intn(50))*time.Millisecond

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			rf.mu.Lock()
			rf.role = Candidate
			rf.mu.Unlock()
			return
		case val := <-rf.heartbeatCh:
			LOGPRINT(INFO, dClient, "No.%v Follower receive heartbeat val = %v\n", rf.me, val)
			if val {
				timer.Reset(timeout)
			} else {
				rf.mu.Lock()
				rf.role = Candidate
				rf.mu.Unlock()
				return
			}
		}
	}

}

func (rf *Raft) runAsCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votes = 1
	rf.votedFor = RoleID(rf.me)
	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   RoleID(rf.me),
	}
	rf.mu.Unlock()

	LOGPRINT(DEBUG, dVote, "No.%v runAsCandidate: votes=%v currentTerm=%v.\n", rf.me, rf.votes, rf.currentTerm)

	for index := range rf.peers {
		if index != rf.me {
			go func(serverid int) {
				LOGPRINT(DEBUG, dVote, "No.%v send to No.%v: CandidateID = %v, CandidateTerm = %v.\n", rf.me, serverid, args.CandidateId, args.CandidateTerm)
				var reply RequestVoteReply
				if rf.sendRequestVote(serverid, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.role != Candidate {
						LOGPRINT(INFO, dVote, "No.%v Im %v now", rf.me, RoleMap[rf.role])
						return
					}
					rf.voteResultCh <- reply
					LOGPRINT(INFO, dVote, "No.%v send reply to voteResultCh(%v) from No.%v", rf.me, len(rf.voteResultCh), serverid)
				}
			}(index)
		}
	}

	timer := time.NewTicker(rf.electTimeout + time.Duration(rand.Intn(150))*time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			rf.mu.Lock()
			rf.VoteClear(rf.currentTerm)
			rf.mu.Unlock()
			return
		case reply := <-rf.voteResultCh:
			rf.mu.Lock()
			LOGPRINT(DEBUG, dVote, "No.%v(CurrentTerm=%v) receive Term-%v VoteGranted = %v [%v/%v]\n", rf.me, rf.currentTerm, reply.VoteTerm, reply.VoteGranted, rf.votes, len(rf.peers))
			if reply.VoteGranted {
				rf.votes++
				if rf.votes > len(rf.peers)/2 {
					rf.role = Leader
					LOGPRINT(DEBUG, dVote, "No.%v become new leader.\n", rf.me)
					rf.VoteChClear()
					rf.mu.Unlock()
					return
				}
			} else if reply.VoteTerm > rf.currentTerm {
				rf.VoteClear(reply.VoteTerm)
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) runAsLeader() {
	ticker := time.NewTicker(rf.heartbeatInterval)
	defer ticker.Stop()

	LOGPRINT(DEBUG, dLeader, "No.%v runAsLeader.\n", rf.me)

	for {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.SendHeartsBeats()
		<-ticker.C
	}
}

func (rf *Raft) SendHeartsBeats() {
	rf.mu.Lock()
	LOGPRINT(INFO, dLeader, "No.%v SendHeartBeats.\n", rf.me)
	args := AppendEntriesArgs{
		LeaderTerm: rf.currentTerm,
		LeaderID:   RoleID(rf.me),
	}
	rf.mu.Unlock()
	for index := range rf.peers {
		if index != rf.me {
			go func(server int) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(server, &args, &reply)
			}(index)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		role := rf.role
		LOGPRINT(DEBUG, dClient, "No.%v Role %v\n", rf.me, rf.role)
		rf.mu.Unlock()
		switch role {
		case Follower:
			rf.runAsFollower()
		case Candidate:
			rf.runAsCandidate()
		case Leader:
			rf.runAsLeader()
		}

		//time.Sleep(rf.electTimeout)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votes = 0
	rf.votedFor = -1
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.electTimeout = 300 * time.Millisecond
	rf.heartbeatCh = make(chan bool, 1)
	rf.voteResultCh = make(chan RequestVoteReply, len(peers)-1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	log.Printf("Make No.%v Success, start ticker.\n", me)

	return rf
}
