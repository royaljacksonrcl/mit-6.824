package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	TaskMap    TaskType = iota
	TaskReduce          //Reduce 任务
	TaskWait            //上一个阶段任务处理中，等待进入下阶段（Task 分配完毕 等待执行结束）
	TaskEnd             //所有任务执行结束
)

type GetTaskArgs struct {
	Id int //默认为 0，表示随机分配任务，非 0 表示指定 Task ID 分配任务
}

type GetTaskReply struct {
	TaskType TaskType
	FileName string //输入文件名（Map 任务使用）
	TaskID   int    //任务 ID
	NMap     int    // Map 的 Task 数量，可以用来确认 Map 处理的完整性
	NReduce  int    //处理 Map Task 时用于将处理完成的 任务分配给 ReduceTask
}

type ReportArgs struct {
	TaskType  TaskType
	TaskID    int
	IsSuccess bool
}

type ReportReply struct {
	StateChange bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
