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
	Persisted         bool
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
	snapshoting  bool
	snapshot     Snapshot
	snapshotdata []byte

	heartbeatCh  chan bool
	startCh      chan bool
	voteResultCh chan RequestVoteReply
	applyCh      chan ApplyMsg

	//for new applier
	applyCond *sync.Cond

	//Volatile state on all servers
	CommitIndex int // 已经确认提交过的 Log 索引
	LastApplied int // 执行过的 Log 索引

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
	persisted := rf.snapshot.Persisted
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.snapshot.Persisted = true
	e.Encode(rf.snapshot)
	data := w.Bytes()
	if !persisted && len(rf.snapshotdata) > 0 {
		rf.persister.SaveStateAndSnapshot(data, rf.snapshotdata)
	} else {
		rf.persister.SaveRaftState(data)
	}
	LOGPRINT(DEBUG, dPersist, "C.%v snapshot.LastIncludedIndex = %v", rf.me, rf.snapshot.LastIncludedIndex)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm Term
	var votedFor RoleID
	var log []LogEntry
	var snapshot Snapshot
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshot) != nil {
		LOGPRINT(ERROR, dPersist, "Failed to read presisted data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshot = snapshot
		rf.LastApplied = rf.snapshot.LastIncludedIndex
		rf.CommitIndex = rf.LastApplied
	}
}

func (rf *Raft) GetPSRaftSize() int {
	return rf.persister.RaftStateSize()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOGPRINT(DEBUG, dLog, "C.%v Receive Snapshot from Leader.", rf.me)

	//Update my log info
	if rf.CommitIndex < lastIncludedIndex {
		rf.CommitIndex = lastIncludedIndex
	}

	rf.LastApplied = lastIncludedIndex

	if rf.currentTerm < Term(lastIncludedTerm) {
		rf.currentTerm = Term(lastIncludedTerm)
	}

	switch len(rf.applyCh) {
	case 1:
		msg := <-rf.applyCh
		LOGPRINT(DEBUG, dLog, "Drop Current msg cause this waiting msg dose not match the snapshot. %+v", msg)
	default:
	}

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
	VoteID      RoleID
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

	reply.VoteID = RoleID(rf.me)
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
	// Leader's term in order to let follower make sure log consistency,
	// when follower's term is less then leader's term, follower do not need to start a new election
	// because there is a valid leader in the current cluster
	Term         Term
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
				LOGPRINT(DEBUG, dDrop, "C.%v Term is not equal.", rf.me)
				return false
			}
		}
	}
	//LOGPRINT(DEBUG, dDrop, "C.%v DropHistoryEntries End. Index = %v, log = %v, from Index.%v", rf.me, index, rf.log, rf.snapshot.LastIncludedIndex)
	return index < len(rf.log)
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	LOGPRINT(INFO, dLog, "C.%v receive Entries.\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOGPRINT(INFO, dLog, "C.%v Term.%v receive Entries of Term.%v to append.\n", rf.me, rf.currentTerm, args.Term)

	if rf.currentTerm > args.Term {
		// 当前节点的人任期更大时，拒绝收到的请求
		LOGPRINT(DEBUG, dLog, "C.%v Term.%v failure currentTerm larger than requsest(C.%v T.%v).\n", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.LastLogIndex = -1
		return
	}

	//更新当前节点信息
	if rf.currentTerm <= args.Term {
		rf.role = Follower
		rf.votedFor = -1
		rf.votes = 0
		rf.currentTerm = args.Term
	}

	//reset heartsbeats
	LOGPRINT(INFO, dClient, "C.%v receive appendentries %v from C.%v, PrevLogIndex = %v PrevLogTerm = %v", rf.me, args.Entries, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm)
	if rf.role == Follower && len(rf.heartbeatCh) < 1 {
		rf.heartbeatCh <- true
	}

	//日志更新
	ret, refargs := rf.CalcWithSnapshot(*args)

	LOGPRINT(INFO, dClient, "C.%v Calc Result %v", rf.me, refargs)

	if !ret || refargs.PrevLogIndex >= len(rf.log) || refargs.PrevLogTerm != rf.log[refargs.PrevLogIndex].Term {
		LOGPRINT(INFO, dLog2, "C.%v %v PrevLogIndex=%v ret=%v\n", rf.me, rf.log, args.PrevLogIndex, ret)
		reply.Term = rf.currentTerm
		reply.Success = false
		if !rf.snapshot.Persisted {
			reply.LastLogIndex = -1
		} else if refargs.PrevLogIndex >= len(rf.log) {
			reply.LastLogIndex = len(rf.log) - 1 + rf.snapshot.LastIncludedIndex
		} else if refargs.PrevLogTerm != rf.log[refargs.PrevLogIndex].Term {
			tmpLogTerm := rf.log[refargs.PrevLogIndex].Term
			tmpIndex := refargs.PrevLogIndex
			for tmpIndex > 0 && tmpLogTerm == rf.log[tmpIndex].Term && tmpIndex > rf.CommitIndex {
				tmpIndex--
			}
			reply.LastLogIndex = tmpIndex
		}
		return
	}

	if rf.DropHistoryEntries(&refargs) {
		reply.Term = rf.currentTerm
		reply.Success = true
		lastCommitIndex := rf.CommitIndex
		if refargs.LeaderCommit > rf.CommitIndex {
			rf.CommitIndex = MININT(refargs.LeaderCommit, len(rf.log)-1) + rf.snapshot.LastIncludedIndex
		}
		if rf.CommitIndex > lastCommitIndex {
			rf.applyCond.Signal()
		}
		return
	}

	LOGPRINT(INFO, dLog, "C.%v Before %v + %v from %v", rf.me, rf.log, refargs.Entries, refargs.PrevLogIndex+1)
	rf.log = append(rf.log[:refargs.PrevLogIndex+1], refargs.Entries...)
	originCommitIndex := rf.CommitIndex
	if refargs.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = MININT(refargs.LeaderCommit, len(rf.log)-1+rf.snapshot.LastIncludedIndex)
	}
	rf.persist()
	LOGPRINT(INFO, dLog, "C.%v After %v", rf.me, rf.log)

	LOGPRINT(INFO, dLog2, "C.%v %v CommitIndex=%v LastApplied=%v\n", rf.me, rf.log, rf.CommitIndex, rf.LastApplied)

	if rf.LastApplied == originCommitIndex && rf.CommitIndex > originCommitIndex {
		rf.applyCond.Signal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	CurrentTerm Term
	LeaderID    int
	Data        []byte
	State       Snapshot
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	CurrentTerm Term
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOGPRINT(DEBUG, dSnap, "C.%v InstallSnapshot Index.%v", rf.me, args.State.LastIncludedIndex)
	if args.CurrentTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		LOGPRINT(DEBUG, dSnap, "C.%v InstallSnapshot Failed.", rf.me)
		return
	}

	rf.currentTerm = args.CurrentTerm
	rf.votedFor = -1
	rf.role = Follower
	rf.persist()

	if rf.snapshot.Persisted && args.State.LastIncludedIndex <= rf.snapshot.LastIncludedIndex {
		LOGPRINT(DEBUG, dSnap, "C.%v InstallSnapshot Ignored. Cause snapshot Index(%v) <= self snapshot Index(%v)", rf.me, args.State.LastIncludedIndex, rf.snapshot.LastIncludedIndex)
		return
	}
	//if rf.CommitIndex > args.State.LastIncludedIndex {
	// 拒绝旧的快照
	//LOGPRINT(DEBUG, dSnap, "C.%v InstallSnapshot Ignored. Cause CommitIndex(%v) > snapshot Index(%v)", rf.me, rf.CommitIndex, args.State.LastIncludedIndex)
	//return
	//}

	//LOGPRINT(DEBUG, dSnap, "C.%v InstallSnapshot Before:snapshot.Index:%v snapshot.Data:%v LastApplied:%v log:%v ", rf.me, rf.snapshot.LastIncludedIndex, len(rf.snapshotdata), rf.LastApplied, rf.log)
	rf.log = truncateLog(rf.log, args.State.LastIncludedIndex-rf.snapshot.LastIncludedIndex, args.State.LastIncludedTerm)
	rf.snapshot = args.State
	rf.snapshotdata = args.Data
	rf.snapshot.Persisted = false

	//减少无用的 snapshot 轮询消息
	if args.State.LastIncludedIndex > rf.LastApplied {
		rf.applyCond.Signal()
	}
	//LOGPRINT(DEBUG, dSnap, "C.%v InstallSnapshot End:snapshot.Index:%v snapshot.Data:%v LastApplied:%v log:%v ", rf.me, rf.snapshot.LastIncludedIndex, len(rf.snapshotdata), rf.LastApplied, rf.log)
}

func (rf *Raft) CalcWithSnapshot(args AppendEntriesArgs) (bool, AppendEntriesArgs) {
	ret := args
	calced := false
	LOGPRINT(DEBUG, dTest, "C.%v Persisted = %v, PrevLogIndex = %v, LastIncludedIndex = %v.",
		rf.me, rf.snapshot.Persisted, ret.PrevLogIndex, rf.snapshot.LastIncludedIndex)
	if rf.snapshot.Persisted && ret.PrevLogIndex >= rf.snapshot.LastIncludedIndex {
		ret.PrevLogIndex -= rf.snapshot.LastIncludedIndex
		calced = true
	}
	return calced, ret
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
	index := len(rf.log) + rf.snapshot.LastIncludedIndex
	term := rf.currentTerm

	if rf.role != Leader {
		rf.mu.Unlock()
		return -1, int(term), false
	}

	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()

	LOGPRINT(DEBUG, dLog, "C.%v Start Command {%v} Index {%v}", rf.me, command, index)
	//log.Printf("test Start(%v) - %v.\n", command, rf.me)

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
	//fmt.Print("current call kill\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		LastLogIndex:  len(rf.log) + rf.snapshot.LastIncludedIndex - 1,
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
			LOGPRINT(DEBUG, dVote, "C.%v(CurrentTerm=%v) receive Term-%v VoteGranted = %v From C.%v [%v/%v]\n", rf.me, rf.currentTerm, reply.VoteTerm, reply.VoteGranted, reply.VoteID, rf.votes, len(rf.peers))
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
	lastLogIndex := len(rf.log) + rf.snapshot.LastIncludedIndex
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.NextIndex[i] = lastLogIndex
		rf.MatchIndex[i] = 0
	}
	LOGPRINT(DEBUG, dVote, "rf.CommitIndex=%v", rf.CommitIndex)
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

	LOGPRINT(DEBUG, dSnap, "C.%v rf.snapshotdata size = %v.", rf.me, len(rf.snapshotdata))

	args := InstallSnapshotArgs{
		CurrentTerm: rf.currentTerm,
		LeaderID:    rf.me,
		State:       rf.snapshot,
		Data:        rf.snapshotdata,
	}

	rf.mu.Unlock()
	var reply InstallSnapshotReply
	LOGPRINT(INFO, dSnap, "C.%v send INSTALLSNAPSHOT to C.%v, SnapshotIndex = %v.\n", rf.me, server, args.State.LastIncludedIndex)
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		LOGPRINT(INFO, dSnap, "C.%v sendInstallSnapshot reply C.%v Term.%v", rf.me, server, reply.CurrentTerm)
		if rf.NextIndex[server] < args.State.LastIncludedIndex {
			rf.NextIndex[server] = args.State.LastIncludedIndex + 1
		}
	}

}

// 判断是否要发送快照，判断逻辑是当前的日志是否能够满足下一次发送的索引位置，如果不能满足，则需要发送快照
func (rf *Raft) needSendSnapshot(server int) bool {
	return rf.NextIndex[server] <= rf.snapshot.LastIncludedIndex
}

func (rf *Raft) dealAEReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		LOGPRINT(DEBUG, dLog, "C.%v Term failure reply(%v) larger than current(%v).\n", rf.me, reply.Term, rf.currentTerm)
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		rf.MatchIndex[server] = max(rf.MatchIndex[server], match)
		rf.NextIndex[server] = rf.MatchIndex[server] + 1

		LOGPRINT(DEBUG, dLog, "C.%v NextIndex = %v.\n", rf.me, rf.NextIndex)
		LOGPRINT(DEBUG, dLog, "C.%v MatchIndex = %v.\n", rf.me, rf.MatchIndex)
		rf.updateCommitIndex()
	} else {
		//异常情况下 更新 nextIndex
		if reply.LastLogIndex >= 0 {
			rf.NextIndex[server] = reply.LastLogIndex + 1
			LOGPRINT(DEBUG, dLog, "C.%v update Server %v NextIndex = %v", rf.me, server, rf.NextIndex[server])
		} else {
			// 快照未完成，回退到上一次的 PrevLogIndex，等待快照发送成功后的更新
			rf.NextIndex[server] = args.PrevLogIndex
			LOGPRINT(DEBUG, dLog, "C.%v update Server %v NextIndex -1 --> %v", rf.me, server, rf.NextIndex[server])
		}
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

				if rf.needSendSnapshot(server) {
					rf.mu.Unlock()
					go rf.SendSnapshot(server)
					return
				}

				LOGPRINT(INFO, dLeader, "C.%v SendAppendEntry to C.%v, CommitIndex = %v.\n", rf.me, server, rf.CommitIndex)
				prevLogIndex := rf.NextIndex[server] - 1
				logpos := prevLogIndex - rf.snapshot.LastIncludedIndex
				if logpos < 0 {
					prevLogIndex = rf.snapshot.LastIncludedIndex
					logpos = 0
				}
				prevLogTerm := rf.log[logpos].Term
				loglen := len(rf.log[logpos+1:])

				LOGPRINT(DEBUG, dLog, "C.%v Log Append : From prevLog.%v-%v to end len = %v", rf.me, prevLogIndex, prevLogTerm, loglen)

				entries := make([]LogEntry, loglen)
				if loglen > 0 {
					copy(entries, rf.log[logpos+1:])
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
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
					if rf.role != Leader {
						LOGPRINT(DEBUG, dLog, "C.%v I'm not Leader anymore. I'm %v now.\n", rf.me, RoleMap[rf.role])
						return
					}
					rf.dealAEReply(server, &args, &reply)
				}
			}(index)
		}
	}
}

func (rf *Raft) snapshotApplied() bool {
	return rf.snapshot.LastIncludedIndex <= rf.LastApplied
}

func (rf *Raft) updateCommitIndex() {
	LOGPRINT(DEBUG, dLog, "C.%v updateCommitIndex From %v, SnapShot End from %v.", rf.me, rf.CommitIndex, rf.snapshot.LastIncludedIndex)
	//LOGPRINT(DEBUG, dLog, "C.%v Current Log: %v", rf.me, rf.log)
	UpdateIndex := rf.CommitIndex
	for i := rf.CommitIndex + 1 - rf.snapshot.LastIncludedIndex; i < len(rf.log); i++ {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.MatchIndex[j] >= (i+rf.snapshot.LastIncludedIndex) {
				count++
			}
		}
		if rf.log[i].Term == rf.currentTerm && count > len(rf.peers)/2 {
			UpdateIndex = i + rf.snapshot.LastIncludedIndex
		}
	}
	if UpdateIndex > rf.CommitIndex {
		LOGPRINT(DEBUG, dLog, "C.%v updateCommitIndex To %v", rf.me, UpdateIndex)
		rf.CommitIndex = UpdateIndex
		LOGPRINT(DEBUG, dLog, "C.%v persist end", rf.me)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) LogsApplier() {
	LOGPRINT(DEBUG, dLog2, "C.%v Apply Log goroutine start runing.\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.LastApplied >= rf.CommitIndex &&
			rf.snapshotApplied() &&
			!rf.killed() {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			return
		}

		if !rf.snapshotApplied() {
			rf.applySnapshot()
			continue
		}

		rf.LastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.LastApplied,
			Command:      rf.log[rf.LastApplied-rf.snapshot.LastIncludedIndex].Command,
		}
		LOGPRINT(DEBUG, dLog, "C.%v ApplyLogs Index.%v cmd(%v)", rf.me, rf.LastApplied, msg.Command)
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		LOGPRINT(DEBUG, dLog, "C.%v ApplyLogs Index.%v End, need to apply to Index.%v", rf.me, rf.LastApplied, rf.CommitIndex)
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

	rf.applyCond = sync.NewCond(&rf.mu)

	rf.snapshot = Snapshot{LastIncludedIndex: 0, LastIncludedTerm: 0, Persisted: true}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshotdata = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier to deal the committed log entries
	go rf.LogsApplier()

	//log.Printf("Make C.%v Success, start ticker, state(%v), len(%v).\n", me, rf.snapshot, len(rf.snapshotdata))

	return rf
}

func (rf *Raft) CreateSnapshot(snapshotData []byte, lastIncludedIndex int) {
	LOGPRINT(DEBUG, dSnap, "C.%v CreateSnapshot as %v, lastIncludedIndex = %v\n", rf.me, RoleMap[rf.role], lastIncludedIndex)
	lastsnapshot := rf.snapshot

	rf.snapshot = Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  rf.log[lastIncludedIndex-lastsnapshot.LastIncludedIndex].Term,
		Persisted:         false,
	}
	rf.snapshotdata = snapshotData

	//LOGPRINT(DEBUG, dSnap, "C.%v CreateSnapshot Origin log eq %v.\n", rf.me, rf.log)
	rf.log = truncateLog(rf.log, lastIncludedIndex-lastsnapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm)
	//LOGPRINT(DEBUG, dSnap, "C.%v CreateSnapshot Current log eq %v.\n", rf.me, rf.log)

	rf.persist()
	LOGPRINT(DEBUG, dSnap, "C.%v CreateSnapshot End.\n", rf.me)
}

func (rf *Raft) applySnapshot() {
	LOGPRINT(DEBUG, dSnap, "C.%v applySnapshot Start.\n", rf.me)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshotdata,
		SnapshotIndex: rf.snapshot.LastIncludedIndex,
		SnapshotTerm:  int(rf.snapshot.LastIncludedTerm),
	}
	rf.mu.Unlock()
	//可能会阻塞，由于当前的 snapshot 无法应用，
	// 后续的 log 应用也不能执行，由于 log 日志和 snapshot 之间的应用关系没有确定
	rf.applyCh <- msg
	rf.mu.Lock()
	LOGPRINT(DEBUG, dSnap, "C.%v applySnapshot End.\n", rf.me)
}

// function to truncate the log
func truncateLog(log []LogEntry, trucatedLogIndex int, lastIncludedTerm Term) []LogEntry {
	newLog := []LogEntry{
		{Term: lastIncludedTerm}}
	if trucatedLogIndex < len(log) {
		newLog = append(newLog, log[trucatedLogIndex+1:]...)
	}
	return newLog
}
