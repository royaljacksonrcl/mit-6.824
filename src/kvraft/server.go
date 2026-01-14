package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ServicePrintf(format string, a ...interface{}) {
	// Uncomment for debugging
	prefix := "[Service] "
	format = prefix + format
	DPrintf(format, a...)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string //Put, Append, Get
	Key       string
	Value     string //for Put && Append
	ClientId  int64  //unique identification for each client
	RequestId int    //unique identification for each request
}

type OpResult struct {
	Err       Err
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore                  map[string]string
	lastAppliedSnapshotIndex int // for snapshot

	lastAppliedCmd map[int64]int
	resultChnl     map[int]chan OpResult
	clientReqId    map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//wait for command apply
	ServicePrintf("[C.%v][Index.%v]Waiting for Get commit.", kv.me, index)
	kv.mu.Lock()
	if _, ok := kv.resultChnl[index]; !ok {
		kv.resultChnl[index] = make(chan OpResult)
	}
	ResChnl := kv.resultChnl[index]
	kv.mu.Unlock()

	for {
		select {
		case result := <-ResChnl:
			if args.ClientId != result.ClientId || args.RequestId != result.RequestId {
				ServicePrintf("[C.%v][Index.%v]Get Req %+v get wrong result %+v of other Client(%v:%v).", kv.me, index, command, result, result.ClientId, result.RequestId)
				// do not return anything, let it timeout
				// do not delete channel here, let the timeout case do it
				continue
			}
			reply.Err = result.Err
			reply.Value = result.Value
			ServicePrintf("[C.%v][Index.%v]End return %v. %v", kv.me, index, result, command)
			kv.mu.Lock()
			delete(kv.resultChnl, index)
			kv.mu.Unlock()
			return
		case <-time.After(300 * time.Millisecond):
			ServicePrintf("[C.%v][Index.%v]Get Req %+v timeout.", kv.me, index, command)
			reply.Err = ErrTimeout
			kv.mu.Lock()
			delete(kv.resultChnl, index)
			kv.mu.Unlock()
			return
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	ServicePrintf("[C.%v][Index.%v]PutAppend Req: %+v. Start return with %d, %v", kv.me, index, op, index, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ServicePrintf("[C.%v][Index.%v]Waiting for op commit.", kv.me, index)
	kv.mu.Lock()
	if _, ok := kv.resultChnl[index]; !ok {
		kv.resultChnl[index] = make(chan OpResult)
	}
	ResChnl := kv.resultChnl[index]
	kv.mu.Unlock()

	for {
		select {
		case result := <-ResChnl:
			// 收到结果
			if args.ClientId != result.ClientId || args.RequestId != result.RequestId {
				ServicePrintf("[C.%v][Index.%v]PutAppend Req %+v get wrong result %+v of other Client(%v:%v).", kv.me, index, op, result, result.ClientId, result.RequestId)
				// do not return anything, let it timeout
				// do not delete channel here, let the timeout case do it
				continue
			}
			ServicePrintf("[C.%v][Index.%v]End return %v. %v", kv.me, index, result, op)
			reply.Err = result.Err
			kv.mu.Lock()
			delete(kv.resultChnl, index)
			kv.mu.Unlock()
			return
		case <-time.After(300 * time.Millisecond):
			ServicePrintf("[C.%v][Index.%v]PutAppend Req %+v timeout.", kv.me, index, op)
			reply.Err = ErrTimeout
			kv.mu.Lock()
			delete(kv.resultChnl, index)
			kv.mu.Unlock()
			return
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvstore = make(map[string]string)
	kv.lastAppliedCmd = make(map[int64]int)
	kv.resultChnl = make(map[int]chan OpResult)
	kv.clientReqId = make(map[int64]int)

	go kv.DealAppliedCmd()

	return kv
}

func (kv *KVServer) DealAppliedCmd() {
	for msg := range kv.applyCh {
		if msg.SnapshotValid {
			kv.mu.Lock()
			ServicePrintf("[C.%v]Receive Snapshot from Raft at Index %v, Current Snapshot Index %v", kv.me, msg.SnapshotIndex, kv.lastAppliedSnapshotIndex)
			//检查快照的有效性并加载快照内容，当重复加载的快照索引小于等于当前快照索引时，说明该快照是旧快照，直接忽略
			if msg.SnapshotIndex <= kv.lastAppliedSnapshotIndex {
				kv.mu.Unlock()
				continue
			}
			//需要注意产生的 Snapshot 可能是其他 Follower 的，当自己被选举为 Leader 后，不再接收旧的 Snapshot

			//decode snapshot
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var kvstore map[string]string
			var lastAppliedCmd map[int64]int
			if d.Decode(&kvstore) != nil || d.Decode(&lastAppliedCmd) != nil {
				log.Fatalf("C.%v Failed to decode snapshot data", kv.me)
			} else {
				kv.kvstore = kvstore
				kv.lastAppliedCmd = lastAppliedCmd
				kv.lastAppliedSnapshotIndex = msg.SnapshotIndex
				ServicePrintf("[C.%v]Load Snapshot from Raft at Index %v Success. KVStore:%v LastAppliedCmd:%v", kv.me, msg.SnapshotIndex, kv.kvstore, kv.lastAppliedCmd)
			}
			kv.mu.Unlock()
			continue
		} else if msg.CommandValid {
			cmd := msg.Command.(Op)
			ServicePrintf("[C.%v][Index.%v]Applied Cmd [%v]", kv.me, msg.CommandIndex, cmd)
			kv.mu.Lock()
			if !kv.isDuplicateRequest(cmd) {
				switch cmd.Type {
				case "Put":
					kv.kvstore[cmd.Key] = cmd.Value
				case "Append":
					kv.kvstore[cmd.Key] += cmd.Value
				}
				kv.lastAppliedCmd[cmd.ClientId] = cmd.RequestId
				// 非重复消息：判断日志的长度并对数据库进行快照，并通知 Raft 进行日志截断
				ServicePrintf("[C.%v]Check maxraftsate %v", kv.me, kv.maxraftstate)
				if kv.maxraftstate != -1 && kv.rf.GetPSRaftSize() > kv.maxraftstate {
					ServicePrintf("[C.%v]Start Snapshot at Index %v", kv.me, msg.CommandIndex)
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.kvstore)
					e.Encode(kv.lastAppliedCmd)
					data := w.Bytes()
					kv.rf.Snapshot(msg.CommandIndex, data)
				}
				ServicePrintf("[C.%v][Index.%v]Notify result Channel.", kv.me, msg.CommandIndex)
			}
			// 重复消息不能跳过，可能执行上次一次的结果没有通知到客户端，需要再次通知客户端
			// 检查当前是否 Leader 状态，处于 Leader 状态才发送结果
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.mu.Unlock()
				continue
			}
			ServicePrintf("[C.%v][Index.%v] Find result Channel.", kv.me, msg.CommandIndex)
			if ch, ok := kv.resultChnl[msg.CommandIndex]; ok {
				var result = OpResult{ClientId: cmd.ClientId, RequestId: cmd.RequestId}
				if cmd.Type == "Get" {
					value, ok := kv.kvstore[cmd.Key]
					if ok {
						result.Value = value
						result.Err = OK
					} else {
						result.Err = ErrNoKey
					}
					ServicePrintf("[C.%v][Index.%v]Get Key=%v Value=%v", kv.me, msg.CommandIndex, cmd.Key, result.Value)
				} else {
					result.Err = OK
				}
				ServicePrintf("[C.%v][Index.%v]Send result(%v) of %v by C.%v", kv.me, msg.CommandIndex, result, cmd, kv.me)
				select {
				case ch <- result:
				default:
					ServicePrintf("[C.%v][Index.%v]Result Channel full, skip sending result %v of %v", kv.me, msg.CommandIndex, result, cmd)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) isDuplicateRequest(op Op) bool {
	lastReqId, ok := kv.lastAppliedCmd[op.ClientId]
	ServicePrintf("[C.%v]Op %v /LastReqId %v.", kv.me, op, lastReqId)
	return ok && op.RequestId <= lastReqId
}
