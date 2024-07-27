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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

const (
	heartbeatInterval time.Duration = 100 * time.Millisecond
	electTimeout      time.Duration = 200 * time.Millisecond
)

func randomTimeout(basic time.Duration, interval time.Duration) time.Duration {
	return basic + time.Duration(rand.Int63n(int64(interval)))
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  Term
	StateMachine      []byte
	persisted         bool
}

func (ss *Snapshot) Encode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*ss)

	return w.Bytes()
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
	role        Role
	currentTerm Term
	votes       int
	votedFor    RoleID
	log         []LogEntry

	//SnapShot
	snapshot Snapshot

	heartbeatCh  chan bool
	startCh      chan bool
	voteResultCh chan RequestVoteReply
	applyCh      chan ApplyMsg

	//Volatile state on all servers
	CommitIndex int
	LastApplied int

	//Volatile state on leader
	NextIndex  []int
	MatchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = int(rf.currentTerm)
	var isleader = (rf.role == Leader)
	// Your code here (2A).
	LOGPRINT(INFO, dTrace, "C.%v return %v %v %v\n", rf.me, term, isleader, rf.role)
	return term, isleader
}

func (rf *Raft) VoteChClear() {
	for {
		select {
		case <-rf.voteResultCh:
		default:
			LOGPRINT(INFO, dVote, "C.%v ClearUseless Votes.\n", rf.me)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	if !rf.snapshot.persisted {
		rf.persister.SaveStateAndSnapshot(data, rf.snapshot.Encode())
		rf.snapshot.persisted = true
	} else {
		rf.persister.SaveRaftState(data)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm Term
	var votedFor RoleID
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		LOGPRINT(ERROR, dPersist, "Failed to read presisted data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LastApplied = lastIncludedIndex

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.snapshot.LastIncludedIndex {
		LOGPRINT(ERROR, dSnap, "index(%v) is out of range(%v), snapshot failed", index, rf.snapshot.LastIncludedIndex)
		return
	}

	rf.CreateSnapshot(snapshot, index)

	rf.ApplySnapshot()
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
	LOGPRINT(INFO, dVote, "C.%v receive Vote Request.\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CandidateTerm > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
		rf.persist()
	}

	reply.VoteTerm = rf.currentTerm
	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	lastLogIndex += rf.snapshot.LastIncludedIndex

	// 如果当前节点还没有投票，并且候选人的任期不小于当前节点的任期，则投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			LOGPRINT(INFO, dVote, "C.%v args.CandidateTerm = %v args.LastLogIndex=%v args.LastLogTerm = %v , rf.currentTerm = %v lastLogIndex = %v lastLogTerm = %v.\n",
				rf.me, args.CandidateTerm, args.LastLogIndex, args.LastLogTerm, rf.currentTerm, lastLogIndex, lastLogTerm)
			LOGPRINT(DEBUG, dVote, "C.%v(CurrentTerm=%v) agree to vote for C.%v\n", rf.me, rf.currentTerm, rf.votedFor)
			if rf.role == Follower && len(rf.heartbeatCh) < 1 {
				rf.heartbeatCh <- true
			}
			rf.persist()
		} else {
			reply.VoteGranted = false
			LOGPRINT(INFO, dVote, "C.%d(CurrentTerm=%v) refused to vote for %d, because of {Index.%v Term.%v} {%v %v}", rf.me, rf.currentTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm)
		}
	} else {
		reply.VoteGranted = false
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			LOGPRINT(INFO, dVote, "C.%d(CurrentTerm=%v) refused to vote for %d, already voted for %d\n", rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)
		}
	}
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	LeaderTerm   Term
	LeaderID     RoleID
	PrevLogIndex int
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term         Term
	Success      bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	LastLogIndex int
}

type LogEntry struct {
	Term    Term
	Command interface{}
}

func (rf *Raft) DropHistoryEntries(args *AppendEntriesArgs) bool {
	index := 0
	for i := 0; i < len(args.Entries); i++ {
		index = args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term != args.Entries[i].Term {
				return false
			}
		}
	}
	return index < len(rf.log)
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	LOGPRINT(INFO, dLog, "C.%v receive Entries.\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOGPRINT(INFO, dLog, "C.%v Term.%v receive Entries of Term.%v to append.\n", rf.me, rf.currentTerm, args.LeaderTerm)

	if rf.currentTerm > args.LeaderTerm {
		// 当前节点的人任期更大时，拒绝收到的请求
		LOGPRINT(DEBUG, dLog, "C.%v Term.%v failure currentTerm larger than requsest(C.%v T.%v).\n", rf.me, rf.currentTerm, args.LeaderID, args.LeaderTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.LastLogIndex = -1
		return
	}

	//更新当前节点信息
	if rf.currentTerm < args.LeaderTerm {
		rf.role = Follower
		rf.votes = 0
		rf.currentTerm = args.LeaderTerm
	}

	//reset heartsbeats
	LOGPRINT(INFO, dClient, "C.%v receive appendentries %v from C.%v, PrevLogIndex = %v PrevLogTerm = %v", rf.me, args.Entries, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm)
	if rf.role == Follower && len(rf.heartbeatCh) < 1 {
		rf.heartbeatCh <- true
	}

	//日志更新
	if args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		LOGPRINT(INFO, dLog2, "C.%v %v PrevLogIndex=%v\n", rf.me, rf.log, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.PrevLogIndex >= len(rf.log) {
			reply.LastLogIndex = len(rf.log) - 1 + rf.snapshot.LastIncludedIndex
		} else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			tmpLogTerm := rf.log[args.PrevLogIndex].Term
			tmpIndex := args.PrevLogIndex
			for tmpIndex > 0 && tmpLogTerm == rf.log[tmpIndex].Term && tmpIndex > rf.CommitIndex {
				tmpIndex--
			}
			reply.LastLogIndex = tmpIndex
		}
		return
	}

	if rf.DropHistoryEntries(args) {
		reply.Term = rf.currentTerm
		reply.Success = true
		if args.LeaderCommit > rf.CommitIndex {
			rf.CommitIndex = MININT(args.LeaderCommit, len(rf.log)-1)
		}
		go rf.ApplyLogs()
		return
	}

	LOGPRINT(INFO, dLog, "C.%v Before %v + %v from %v", rf.me, rf.log, args.Entries, args.PrevLogIndex+1)
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = MININT(args.LeaderCommit, len(rf.log)-1)
	}
	rf.persist()
	LOGPRINT(INFO, dLog, "C.%v After %v", rf.me, rf.log)

	LOGPRINT(INFO, dLog2, "C.%v %v CommitIndex=%v LastApplied=%v\n", rf.me, rf.log, rf.CommitIndex, rf.LastApplied)

	go rf.ApplyLogs()

	reply.Term = rf.currentTerm
	reply.Success = true
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	CurrentTerm Term
	LeaderID    int
	Data        Snapshot
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	CurrentTerm Term
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CurrentTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		return
	}

	rf.currentTerm = args.CurrentTerm
	rf.votedFor = -1
	rf.role = Follower
	rf.persist()

	if args.Data.LastIncludedIndex <= rf.snapshot.LastIncludedIndex {
		return
	}

	rf.log = truncateLog(rf.log, args.Data.LastIncludedIndex-rf.snapshot.LastIncludedIndex, args.Data.LastIncludedTerm)
	rf.snapshot = args.Data
	rf.snapshot.persisted = false

	rf.persist()

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	// Your code here (2B).
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm

	if rf.role != Leader {
		rf.mu.Unlock()
		return -1, int(term), false
	}

	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()

	LOGPRINT(DEBUG, dLog, "C.%v Start Command {%v} Index {%v}", rf.me, command, index)
	fmt.Printf("test Start(%v) - %v.\n", command, rf.me)

	if len(rf.startCh) < 1 && !rf.killed() {
		rf.startCh <- true
	}
	rf.mu.Unlock()
	//rf.SendAppendEntry()

	return index, int(term), true
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
	close(rf.startCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runAsFollower() {
	LOGPRINT(DEBUG, dClient, "C.%v runAsFollower\n", rf.me)
	//timeout := rf.heartbeatInterval*2 + time.Duration(rand.Intn(100))*time.Millisecond
	timeout := randomTimeout(heartbeatInterval*2, heartbeatInterval)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			rf.mu.Lock()
			if len(rf.heartbeatCh) == 0 {
				rf.role = Candidate
				rf.VoteChClear()
			}
			rf.mu.Unlock()
			return
		case val := <-rf.heartbeatCh:
			LOGPRINT(INFO, dClient, "C.%v Follower receive heartbeat val = %v\n", rf.me, val)
			if val {
				if !timer.Stop() {
					<-timer.C
				}
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
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.votes = 1
	rf.votedFor = RoleID(rf.me)
	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   RoleID(rf.me),
		LastLogIndex:  len(rf.log) - 1,
		LastLogTerm:   rf.log[len(rf.log)-1].Term,
	}
	rf.persist()
	rf.mu.Unlock()

	LOGPRINT(DEBUG, dVote, "C.%v runAsCandidate.\n", rf.me)

	for index := range rf.peers {
		if index != rf.me {
			go func(serverid int) {
				LOGPRINT(DEBUG, dVote, "C.%v send to C.%v: CandidateID = %v, CandidateTerm = %v.\n", rf.me, serverid, args.CandidateId, args.CandidateTerm)
				var reply RequestVoteReply
				if rf.sendRequestVote(serverid, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.role != Candidate {
						LOGPRINT(INFO, dVote, "C.%v Im %v now", rf.me, RoleMap[rf.role])
						return
					}
					rf.voteResultCh <- reply
					LOGPRINT(INFO, dVote, "C.%v receive reply->voteResultCh(%v) from C.%v", rf.me, len(rf.voteResultCh), serverid)
				} else {
					LOGPRINT(INFO, dVote, "C.%v send to C.%v Failed.\n", rf.me, serverid)
				}
			}(index)
		}
	}

	timer := time.NewTimer(randomTimeout(electTimeout, heartbeatInterval))
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			rf.mu.Lock()
			rf.role = Follower
			rf.votedFor = -1
			rf.votes = 0
			rf.persist()
			rf.mu.Unlock()
			return
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role == Candidate {
				rf.role = Follower
			}
			return
		case reply := <-rf.voteResultCh:
			rf.mu.Lock()
			LOGPRINT(DEBUG, dVote, "C.%v(CurrentTerm=%v) receive Term-%v VoteGranted = %v [%v/%v]\n", rf.me, rf.currentTerm, reply.VoteTerm, reply.VoteGranted, rf.votes, len(rf.peers))
			if reply.VoteGranted && reply.VoteTerm == rf.currentTerm {
				rf.votes++
				if rf.votes > len(rf.peers)/2 {
					rf.role = Leader
					LOGPRINT(DEBUG, dVote, "C.%v become new leader.\n", rf.me)
					rf.VoteChClear()
					rf.Reinitialized()
					rf.mu.Unlock()
					return
				}
			} else if reply.VoteTerm > rf.currentTerm {
				rf.role = Follower
				rf.votedFor = -1
				rf.votes = 0
				rf.currentTerm = reply.VoteTerm
				rf.persist()
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Reinitialized() {
	lastLogIndex := len(rf.log)
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.NextIndex[i] = lastLogIndex
		rf.MatchIndex[i] = 0
	}
}

func (rf *Raft) runAsLeader() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	LOGPRINT(DEBUG, dLeader, "C.%v runAsLeader.\n", rf.me)

	loop := 0

	for {
		rf.mu.Lock()
		if rf.role != Leader || rf.killed() {
			LOGPRINT(WARNING, dLeader, "C.%v Term.%v loop=%v is not Leader anymore.\n", rf.me, rf.currentTerm, loop)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		LOGPRINT(DEBUG, dLeader, "[IN]C.%v loop = %v.\n", rf.me, loop)

		rf.SendAppendEntry()

		LOGPRINT(DEBUG, dLeader, "[OUT]C.%v loop = %v.\n", rf.me, loop)

		select {
		case val := <-rf.startCh:
			if val {
				ticker.Reset(heartbeatInterval)
			}
		case <-ticker.C:
		case <-rf.heartbeatCh: //Leader 下收到 心跳 进行处理，避免阻塞
		}
		loop++
	}
}

func (rf *Raft) SendSnapshot(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		CurrentTerm: rf.currentTerm,
		LeaderID:    rf.me,
		Data:        rf.snapshot,
	}

	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.mu.Lock()
	}

}

func (rf *Raft) SendAppendEntry() {
	for index := range rf.peers {
		if index != rf.me {
			go func(server int) {
				rf.mu.Lock()
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}

				LOGPRINT(INFO, dLeader, "C.%v SendAppendEntry to C.%v, CommitIndex = %v.\n", rf.me, server, rf.CommitIndex)
				prevLogIndex := rf.NextIndex[server] - 1
				if prevLogIndex < rf.snapshot.LastIncludedIndex {
					rf.mu.Unlock()
					go rf.SendSnapshot(server)
					return
				}
				prevLogTerm := rf.log[prevLogIndex-rf.snapshot.LastIncludedIndex].Term
				loglen := len(rf.log[prevLogIndex-rf.snapshot.LastIncludedIndex+1:])

				LOGPRINT(DEBUG, dLog, "C.%v Log Append : From prevLog.%v-%v to end len = %v", rf.me, prevLogIndex, prevLogTerm, loglen)

				entries := make([]LogEntry, loglen)
				if loglen > 0 {
					copy(entries, rf.log[prevLogIndex-rf.snapshot.LastIncludedIndex+1:])
				}
				args := AppendEntriesArgs{
					LeaderTerm:   rf.currentTerm,
					LeaderID:     RoleID(rf.me),
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.CommitIndex,
				}
				rf.mu.Unlock()

				var reply AppendEntriesReply
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					LOGPRINT(DEBUG, dLog, "C.%v Deal with relpy(%v) from C.%v.\n", rf.me, reply.Success, server)
					if reply.Term > rf.currentTerm {
						LOGPRINT(DEBUG, dLog, "C.%v Term failure reply(%v) larger than current(%v).\n", rf.me, reply.Term, rf.currentTerm)
						rf.role = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						return
					}

					if rf.role == Leader && reply.Success {
						if prevLogIndex+1+len(entries) >= rf.NextIndex[server] {
							rf.NextIndex[server] = prevLogIndex + 1 + len(entries)
							rf.MatchIndex[server] = rf.NextIndex[server] - 1
						}
						rf.updateCommitIndex()
					} else {
						if reply.LastLogIndex == -1 {
							rf.NextIndex[server] = prevLogIndex
						} else {
							rf.NextIndex[server] = reply.LastLogIndex + 1
						}
					}

				}
			}(index)
		}
	}
}
func (rf *Raft) updateCommitIndex() {
	LOGPRINT(DEBUG, dLog, "C.%v updateCommitIndex From %v", rf.me, rf.CommitIndex)
	UpdateIndex := rf.CommitIndex
	for i := rf.CommitIndex + 1; i < len(rf.log); i++ {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.MatchIndex[j] >= i {
				count++
			}
		}
		if rf.log[i].Term == rf.currentTerm && count > len(rf.peers)/2 {
			UpdateIndex = i
		}
	}
	if UpdateIndex > rf.CommitIndex {
		LOGPRINT(DEBUG, dLog, "C.%v updateCommitIndex To %v", rf.me, UpdateIndex)
		rf.CommitIndex = UpdateIndex
		rf.persist()
		LOGPRINT(DEBUG, dLog, "C.%v persist end", rf.me)
		go rf.ApplyLogs()
	}
}

func (rf *Raft) ApplyLogs() {
	LOGPRINT(DEBUG, dLog, "C.%v ApplyLogs Start", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.LastApplied < rf.CommitIndex {
		rf.LastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.LastApplied,
			Command:      rf.log[rf.LastApplied-rf.snapshot.LastIncludedIndex].Command,
		}
		LOGPRINT(DEBUG, dLog, "C.%v ApplyLogs Index.%v", rf.me, rf.LastApplied)
		rf.applyCh <- msg
		LOGPRINT(DEBUG, dLog, "C.%v ApplyLogs Index.%v End", rf.me, rf.LastApplied)
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
		LOGPRINT(DEBUG, dClient, "C.%v Role %v\n", rf.me, rf.role)
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
	rf.log = make([]LogEntry, 1)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	rf.heartbeatCh = make(chan bool, 1)
	rf.startCh = make(chan bool, 1)
	rf.voteResultCh = make(chan RequestVoteReply, len(peers)-1)
	rf.applyCh = applyCh

	rf.snapshot = Snapshot{LastIncludedIndex: 0, LastIncludedTerm: 0, StateMachine: nil, persisted: true}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	log.Printf("Make C.%v Success, start ticker.\n", me)

	return rf
}

func (rf *Raft) CreateSnapshot(snapshotData []byte, lastIncludedIndex int) {
	LOGPRINT(DEBUG, dSnap, "C.%v CreateSnapshot as %v, lastIncludedIndex = %v\n", rf.me, RoleMap[rf.role], lastIncludedIndex)
	lastsnapshot := rf.snapshot

	rf.snapshot = Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  rf.log[lastIncludedIndex].Term,
		StateMachine:      snapshotData,
		persisted:         false,
	}

	rf.log = truncateLog(rf.log, lastIncludedIndex-lastsnapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm)

	rf.persist()
	LOGPRINT(DEBUG, dSnap, "C.%v CreateSnapshot End.\n", rf.me)
}

func (rf *Raft) ApplySnapshot() {
	LOGPRINT(DEBUG, dSnap, "C.%v ApplySnapshot Start.\n", rf.me)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot.StateMachine,
		SnapshotIndex: rf.snapshot.LastIncludedIndex,
		SnapshotTerm:  int(rf.snapshot.LastIncludedTerm),
	}
	LOGPRINT(DEBUG, dSnap, "C.%v ApplySnapshot End.\n", rf.me)
}

// function to truncate the log
func truncateLog(log []LogEntry, trucatedLogIndex int, lastIncludedTerm Term) []LogEntry {
	newLog := []LogEntry{
		{Term: lastIncludedTerm}}
	newLog = append(newLog, log[trucatedLogIndex+1:]...)
	return newLog
}
