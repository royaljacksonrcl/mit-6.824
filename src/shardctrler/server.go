package shardctrler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg //Log entries from Raft

	// Your data here.

	// Related to Raft
	configs          []Config              // indexed by config num
	LastAppliedOpIdx map[int64]uint64      // map[ClientId]LastAppliedRequestId]
	resultCh         map[int]chan OpResult // map[LogIndex]chan OpResult, Transmit the result to the waiting RPC handler
}

type OpType string

const (
	Join  OpType = "Join"
	Leave OpType = "Leave"
	Move  OpType = "Move"
	Query OpType = "Query"
)

type Op struct {
	// Your data here.
	Type OpType // "Join", "Leave", "Move", "Query"
	Args BaseArgs
}

type OpResult struct {
	Args   BasicArgs
	Err    Err
	Config Config
}

func (sc *ShardCtrler) Initialize() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.LastAppliedOpIdx = make(map[int64]uint64)
	sc.resultCh = make(map[int]chan OpResult)
	sc.configs = make([]Config, 0)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	Log := Op{
		Type:     Join,
		Args:     *args,
		ClientId: sc.Identify,
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	cmd := Op{
		Type: Query,
		Args: *args,
	}

	// Submit to Raft
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	meta := fmt.Sprintf("[%v][Index.%v]", index, args.ToString())

	result := sc.ProcessOp(index, args.clientId, args.requestId, 300*time.Millisecond)
	reply.Err = reply.Err
	if result.Err == OK {
		reply.Config = result.Config
		return
	}

	LogPrintf(LogApply, meta, "Process Query Operation Timeout.")
}

func (sc *ShardCtrler) DestroyChan(idx int) {
	sc.mu.Lock()
	delete(sc.resultCh, idx)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ProcessOp(index int, clientId int64, reqId uint64, timeout time.Duration) OpResult {
	sc.mu.Lock()
	if _, ok := sc.resultCh[index]; !ok {
		sc.resultCh[index] = make(chan OpResult, 1)
	}
	resultCh := sc.resultCh[index]
	sc.mu.Unlock()

	meta := fmt.Sprintf("[%v/%v][Index.%v]", clientId, reqId, index)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case result := <-resultCh:
			if clientId != result.Args.clientId || reqId != result.Args.requestId {
				LogPrintf(LogApply, meta, "Receive Op result from %v but not match. Discard this result", result.Args.ToString())
				continue
			}
			sc.DestroyChan(index)
			return result
		case <-ctx.Done():
			LogPrintf(LogApply, meta, "Precess Op timeout.")
			sc.DestroyChan(index)
			return OpResult{
				Err: ErrTimeout,
			}
		}
	}

}

func (sc *ShardCtrler) DealMsgApplier() {
	for msg := range sc.applyCh {
		if msg.SnapshotValid {
			sc.mu.Lock()
			if sc.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				meta := fmt.Sprintf("[SS.%v(%v)]", msg.SnapshotIndex, msg.SnapshotTerm)
				LogPrintf(LogSnapshot, meta, "Install Snapshot.")
				//decode snapshot
			}
		} else if msg.CommandValid {
			cmd := msg.Command.(Op)
			var result OpResult
			sc.mu.Lock()
			meta := fmt.Sprintf("[%v][Index.%v]", cmd.Args.ToString(), msg.CommandIndex)
			LogPrintf(LogApply, meta, "Log Appling.")
			if !sc.isDuplicateRequest(cmd) {
				result = sc.op_execute(cmd)
			} else {
				result.Args = cmd.Args.GetIdArgs()
				result.Err = OK
			}

			_, isLeader := sc.rf.GetState()
			if !isLeader {
				sc.mu.Unlock()
				continue
			}

			if ch, ok := sc.resultCh[msg.CommandIndex]; ok {
				ch <- result
			}
		}
	}
}

func (sc *ShardCtrler) isDuplicateRequest(op Op) bool {
	lastReqId, ok := sc.LastAppliedOpIdx[op.Args.GetIdArgs().clientId]
	if ok && op.Args.GetIdArgs().requestId <= lastReqId && op.Type != Query {
		return true
	}
	return false
}

func (sc *ShardCtrler) op_execute(op Op) OpResult {
	return OpResult{
		Args: op.Args.GetIdArgs(),
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// 增加接口类
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})

	// 增加基础类
	labgob.Register(BasicArgs{})

	// Your code here.
	sc.Initialize()

	go sc.DealMsgApplier()

	return sc
}
