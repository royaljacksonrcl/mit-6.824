package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu sync.Mutex
	//leaderId  int 暂时不修改，如果需要提升性能时修改
	clientId  int64 //unique identifycation for each client
	requestId uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.requestId = 0

	return ck
}

func (ck *Clerk) meta() string {
	return fmt.Sprintf("[%v/%v]", ck.clientId, ck.requestId)
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestId += 1
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	meta := ck.meta()

	for {
		// try each known server.
		for idx, srv := range ck.servers {
			var reply QueryReply
			LogPrintf(LogRPC, meta, "Query No.%v to S.%v", args.Num, idx)
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				LogPrintf(LogRPC, meta, "Query return End.")
				return reply.Config
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestId += 1
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestId += 1
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestId += 1
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
