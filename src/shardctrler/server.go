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
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	cmd := Op{
		Type: Join,
		Args: *args,
	}

	// Submit to Raft
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

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
	sc.mu.Lock()
	cmd := Op{
		Type: Leave,
		Args: *args,
	}

	// Submit to Raft
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	meta := fmt.Sprintf("[%v][Index.%v]", index, args.ToString())

	result := sc.Processing(index, args.ClientId, args.RequestId, 300*time.Millisecond)
	reply.Err = result.Err
	if result.Err == OK {
		return
	}

	LogPrintf(LogApply, meta, "Process Query Operation Timeout.")
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	cmd := Op{
		Type: Move,
		Args: *args,
	}

	// Submit to Raft
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	meta := fmt.Sprintf("[%v][Index.%v]", index, args.ToString())

	result := sc.Processing(index, args.ClientId, args.RequestId, 300*time.Millisecond)
	reply.Err = result.Err
	if result.Err == OK || result.Err == ErrNoKey || result.Err == ErrNotSupport {
		return
	}

	LogPrintf(LogApply, meta, "Process Query Operation Timeout.")
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	cmd := Op{
		Type: Query,
		Args: *args,
	}

	// Submit to Raft
	index, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	meta := fmt.Sprintf("[%v][Index.%v]", args.ToString(), index)

	LogPrintf(LogApply, meta, "Processing:Waiting for OpResult.")
	result := sc.Processing(index, args.ClientId, args.RequestId, 300*time.Millisecond)
	reply.Err = result.Err
	if result.Err != ErrTimeout {
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
			LogPrintf(LogApply, meta, "Op is completed with result %+v", result)
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
		meta := fmt.Sprintf("[C.%v]", sc.Raft().Getme())
		LogPrintf(LogApply, meta, "Receive msg.")
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
			meta := fmt.Sprintf("[%v][C.%v][Index.%v]", cmd.Args.ToString(), sc.Raft().Getme(), msg.CommandIndex)
			LogPrintf(LogApply, meta, "Log Appling.")
			sc.mu.Lock()
			LogPrintf(LogApply, meta, "Log Appling Start.")
			if !sc.isDuplicateRequest(cmd) {
				LogPrintf(LogApply, meta, "Start to execute command.")
				result = sc.op_execute(cmd)
			} else {
				result.Args = cmd.Args.GetIdArgs()
				result.Err = OK
			}

			_, isLeader := sc.rf.GetState()
			if !isLeader {
				LogPrintf(LogApply, meta, "current rf is not leader, discard the result.")
				sc.mu.Unlock()
				continue
			}
			LogPrintf(LogApply, meta, "Send result(%+v) to Channel.", result)
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
	meta := fmt.Sprintf("[Rebalance][C.%v][Config.%v]", sc.Raft().Getme(), config.Groups)
	LogPrintf(LogDebug, meta, "Start to rebalance.")

	grp_size := len(config.Groups)

	if grp_size == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}

	average := NShards / grp_size
	extra := NShards % grp_size

	// 更新切片数量观察是否达到平均值，多余的切片分配给随机的组
	// 增加 group 时需要将超过平均数的 shard 提取出来
	// 减少 group 时将删除的 gid 的 shard 筛选出来
	// 这两部分合并在同一个循环里以提高性能
	waiting_shards := make([]int, 0)
	grp_shards := make(map[int][]int) // map[gid][]shardIdx
	for idx, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			config.Shards[idx] = 0
			waiting_shards = append(waiting_shards, idx)
			continue
		}

		group_counts := len(grp_shards[gid])
		if group_counts > average {
			config.Shards[idx] = 0
			waiting_shards = append(waiting_shards, idx)
		} else {
			// 分 3 种情况
			// - 小于平局数时，统计当前 gid 被分配的 shards 数量
			// - 等于平均数时，当 extra 大于 0 时，保留当前的 gid 的分配 shard，并对 extra 自减 进行分配确认，并记录到 gid 的分配 shards 数量中
			// - 大于平局数时，将当前 gid 对应的 shard 移动到等待分组的队列中，并重置 shard 的对应 gid 为 0，确保 shard 的 gid 不会指向已经移除的 group
			if group_counts < average {
				grp_shards[gid] = append(grp_shards[gid], idx)
			} else if group_counts == average && extra > 0 {
				extra -= 1
				grp_shards[gid] = append(grp_shards[gid], idx)
			} else {
				config.Shards[idx] = 0
				waiting_shards = append(waiting_shards, idx)
			}
		}
	}

	LogPrintf(LogDebug, meta, "Rebalance finish to find unassigned shards %+v, current group shards %+v.", waiting_shards, grp_shards)

	// 将没有分配的切片进行重新分配
	for _, idx := range waiting_shards {
		for gid := range config.Groups {
			counts := len(grp_shards[gid])
			if counts < average || (counts == average && extra > 0) {
				config.Shards[idx] = gid
				grp_shards[gid] = append(grp_shards[gid], idx)
				if counts == average {
					extra -= 1
				}
				break
			}
		}
	}

	LogPrintf(LogApply, "[Rebalance]", "Rebalance completed with new shard distribution %+v.", grp_shards)
}

func CopyGroups(org map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range org {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func (sc *ShardCtrler) join_exec(args JoinArgs) Err {
	var last_shards [NShards]int
	var last_groups map[int][]string
	last_shards = sc.configs[len(sc.configs)-1].Shards
	last_groups = sc.configs[len(sc.configs)-1].Groups

	meta := fmt.Sprintf("[%v][C.%v]", args.ToString(), sc.Raft().Getme())

	new_config := Config{
		Num:    len(sc.configs),
		Shards: last_shards, // 复制上一个切片记录
		Groups: CopyGroups(last_groups),
	}

	for gid, servers := range args.Servers {
		new_config.Groups[gid] = servers
	}

	LogPrintf(LogApply, meta, "Run rebalance with new config %+v.", new_config)
	sc.rebalance(&new_config)
	LogPrintf(LogApply, meta, "rebalance completed.")

	sc.configs = append(sc.configs, new_config)
	return OK
}

func (sc *ShardCtrler) move_exec(args MoveArgs) Err {
	return ErrNotSupport
}

func (sc *ShardCtrler) leave_exec(args LeaveArgs) Err {
	var last_shards [NShards]int
	var last_groups map[int][]string
	if len(sc.configs) > 0 {
		last_shards = sc.configs[len(sc.configs)-1].Shards
		last_groups = sc.configs[len(sc.configs)-1].Groups
	} else {
		last_groups = make(map[int][]string)
	}

	meta := fmt.Sprintf("[%v][C.%v]", args.ToString(), sc.Raft().Getme())

	new_config := Config{
		Num:    len(sc.configs),
		Shards: last_shards, // 复制上一个切片记录
		Groups: CopyGroups(last_groups),
	}

	LogPrintf(LogApply, meta, "Start to leave %+v.", args.GIDs)

	for _, gid := range args.GIDs {
		LogPrintf(LogApply, meta, "Check Leave %v.", gid)
		if v, ok := new_config.Groups[gid]; ok {
			LogPrintf(LogApply, meta, "Leave %v from config. Removed group contains %v.", gid, v)
			delete(new_config.Groups, gid)
		}
		LogPrintf(LogApply, meta, "Finish Leave %v, current Groups %+v.", gid, new_config.Groups)
	}

	LogPrintf(LogApply, meta, "Finish Leave and rebalance with new config %+v.", new_config)

	sc.rebalance(&new_config)

	sc.configs = append(sc.configs, new_config)

	return OK
}

func (sc *ShardCtrler) query_exec(args QueryArgs, result *OpResult) Err {
	meta := fmt.Sprintf("[%v/%v]", args.ClientId, args.RequestId)
	LogPrintf(LogApply, meta, "Start to Deal Query Command.")
	result.Args = args.GetIdArgs()

	if args.Num == -1 {
		if len(sc.configs) > 0 {
			last_config := sc.configs[len(sc.configs)-1]
			result.Config = last_config
			LogPrintf(LogApply, meta, "Get Last Config %+v.", result.Config)
			return OK
		} else {
			result.Config = Config{}
			result.Err = ErrMisMatch
			LogPrintf(LogApply, meta, "Query Num MisMatched.")
			return ErrMisMatch
		}
	}

	if args.Num < 0 || args.Num >= len(sc.configs) {
		result.Err = ErrNoKey
		LogPrintf(LogApply, meta, "Query Num is out of range.")
		return ErrNoKey
	}

	result.Config = sc.configs[args.Num]
	LogPrintf(LogApply, meta, "Get No.%v Config %+v.", args.Num, result.Config)

	return OK
}

func (sc *ShardCtrler) op_execute(op Op) OpResult {
	result := OpResult{
		Args: op.Args.GetIdArgs(),
	}
	meta := fmt.Sprintf("[%v]", op.Args.ToString())
	LogPrintf(LogApply, meta, "Start to execute %v operation.", op.Type)

	switch op.Type {
	case Join:
		args := op.Args.(JoinArgs)
		result.Err = sc.join_exec(args)
	case Move:
		args := op.Args.(MoveArgs)
		result.Err = sc.move_exec(args)
	case Leave:
		args := op.Args.(LeaveArgs)
		result.Err = sc.leave_exec(args)
	case Query:
		args := op.Args.(QueryArgs)
		result.Err = sc.query_exec(args, &result)
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

	// 日志模块初始化
	topiclist := StringToTopic(getDebugModule())
	initLogger(topiclist...)

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
