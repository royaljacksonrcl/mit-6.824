package shardctrler

import (
	"fmt"
	"os"
	"strings"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoKey       = "ErrNoKey"
	ErrTimeout     = "ErrTimeout"
	ErrMisMatch    = "ErrMisMatch"
)

type Err string

// 接口
type BaseArgs interface {
	GetIdArgs() BasicArgs
	ToString() string
}

// 实例基础参数
type BasicArgs struct {
	ClientId  int64
	RequestId uint64
}

func (b BasicArgs) GetIdArgs() BasicArgs {
	return BasicArgs{
		ClientId:  b.ClientId,
		RequestId: b.RequestId,
	}
}

func (b BasicArgs) ToString() string {
	return fmt.Sprintf("%v/%v", b.ClientId, b.RequestId)
}

type JoinArgs struct {
	BasicArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	BasicArgs
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	BasicArgs
	GIDs []int
}

type LeaveReply struct {
	BasicArgs
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	BasicArgs
	Shard int
	GID   int
}

type MoveReply struct {
	BasicArgs
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	BasicArgs
	Num int // desired config number
}

type QueryReply struct {
	BasicArgs
	WrongLeader bool
	Err         Err
	Config      Config
}

func getDebugModule() []string {
	osEnv := os.Getenv("DEBUG")
	if osEnv == "" {
		return []string{}
	}
	return strings.Split(osEnv, ",")
}
