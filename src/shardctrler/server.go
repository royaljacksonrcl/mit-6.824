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

	cmd := Op{
		Type: Join,
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

	result := sc.Processing(index, args.ClientId, args.RequestId, 300*time.Millisecond)
	reply.Err = result.Err
	if result.Err == OK {
		return
	}

	LogPrintf(LogApply, meta, "Process Query Operation Timeout.")

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

	result := sc.Processing(index, args.ClientId, args.RequestId, 300*time.Millisecond)
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

func (sc *ShardCtrler) Processing(index int, clientId int64, reqId uint64, timeout time.Duration) OpResult {
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
			if clientId != result.Args.ClientId || reqId != result.Args.RequestId {
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
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) isDuplicateRequest(op Op) bool {
	lastReqId, ok := sc.LastAppliedOpIdx[op.Args.GetIdArgs().ClientId]
	if ok && op.Args.GetIdArgs().RequestId <= lastReqId && op.Type != Query {
		return true
	}
	return false
}

func (sc *ShardCtrler) rebalance(config *Config) {
	grp_size := len(config.Groups)

	if grp_size == 0 {
		for i :=0 ; i < NShards; i++ {
			config.Shards[i] = 0
		}
	}

	average := NShards / grp_size
	extra := NShards % grp_size

	// 更新切片数量观察是否达到平均值，多余的切片分配给随机的组
	waiting_shards := make([]int, 0)
	grp_shards := make(map[int][]int) // map[gid][]shardIdx
	for idx, gid := range config.Shards {
		if _, ok := config.Groups[gid]; ok {
			grp_shards[gid] = append(grp_shards[gid], idx)
		} else {
			config.Shards[idx] = 0 
			waiting_shards = append(waiting_shards, idx)
		}
	}

	// 增加 group 数量，分配切片
	for gid, _ := range config.Groups {
		if _, ok := grp_shards[gid]; !ok {
			grp_shards[gid] = make([]int, )
		} else {
			counts := len(grp_shards[gid])
			if counts > average {
				need_remove_size = counts - average
				if extra > 0 {
					need_remove_size -= 1
					extra -= 1
				}
				for i := 0; i < need_remove_size; i++ {
					waiting_shards = append(waiting_shards, grp_shards[gid][counts-1-i])
				}
			}
		}
	}


	// 减少 group 数量，将没有分配的切片进行重新分配
	for shard_idx := range waiting_shards {
		for gid, shards := range grp_shards {
			counts := len(shards)
			if counts < average || (counts == average && extra > 0) {
				config.Shards[shard_idx] = gid
				grp_counts[gid] += 1
				if counts == average {
					extra -= 1
				}
				break
			}
		}
	}
}

func (sc *ShardCtrler) join_exec(args JoinArgs) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	new_config := Config{
		Num:    len(sc.configs),
		Shards: lastConfig.Shards, // 复制上一个切片记录
		Groups: make(map[int][]string),
	}

	for gid, servers := range args.Servers {
		new_config.Groups[gid] = servers
	}

	sc.rebalance(&new_config)

	sc.configs = append(sc.configs, new_config)
	return OK
}

func (sc *ShardCtrler) query_exec(args QueryArgs, result OpResult) Err {
	if args.Num == -1 {
		last_config := sc.configs[]
	}

	return OK
}

func (sc *ShardCtrler) op_execute(op Op) OpResult {
	result := OpResult{
		Args: op.Args.GetIdArgs(),
	}

	switch op.Type {
	case Join:
		args := op.Args.(JoinArgs)
		result.Err = sc.join_exec(args)
	case Move:
	case Leave:
	case Query:
		Args := op.Args.(QueryArgs)
		result.Err = sc.query_exec(args)
	}

	return result
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
