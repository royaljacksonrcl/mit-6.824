package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

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
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore map[string]string

	lastAppliedCmd map[int64]int
	resultChnl     map[int]chan OpResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{Type: "Get", Key: args.Key}
	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//wait for command apply
	kv.mu.Lock()
	if _, ok := kv.resultChnl[index]; !ok {
		kv.resultChnl[index] = make(chan OpResult)
	}
	ResChnl := kv.resultChnl[index]
	kv.mu.Unlock()

	select {
	case result := <-ResChnl:
		reply.Err = result.Err
		reply.Value = result.Value
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
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if _, ok := kv.resultChnl[index]; !ok {
		kv.resultChnl[index] = make(chan OpResult)
	}
	ResChnl := kv.resultChnl[index]
	kv.mu.Unlock()

	select {
	case result := <-ResChnl:
		reply.Err = result.Err
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

	go kv.DealAppliedCmd()

	return kv
}

func (kv *KVServer) DealAppliedCmd() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			DPrintf("Applied Cmd [%v]", cmd)
			kv.mu.Lock()
			if !kv.isDuplicateRequest(cmd) {
				if cmd.Type == "Put" {
					kv.kvstore[cmd.Key] = cmd.Value
				} else if cmd.Type == "Append" {
					kv.kvstore[cmd.Key] += cmd.Value
				}
				kv.lastAppliedCmd[cmd.ClientId] = cmd.RequestId
			}
			if ch, ok := kv.resultChnl[msg.CommandIndex]; ok {
				var result OpResult
				if cmd.Type == "Get" {
					value, ok := kv.kvstore[cmd.Key]
					if ok {
						result.Value = value
						result.Err = OK
					} else {
						result.Err = ErrNoKey
					}
				} else {
					result.Err = OK
				}
				ch <- result
				close(ch)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) isDuplicateRequest(op Op) bool {
	lastReqId, ok := kv.lastAppliedCmd[op.ClientId]
	return ok && op.RequestId <= lastReqId
}
