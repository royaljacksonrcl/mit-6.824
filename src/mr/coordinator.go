package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type State int

const (
	Idle       State = iota //空闲
	Processing              //处理中
	Complete                //处理完成
	Failed                  //彻底失败
)

type Task struct {
	id        int
	fileName  string
	taskState State
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	_mu     sync.Mutex
	_ctx    context.Context    //上下文信息
	_cancel context.CancelFunc //取消当前任务

	files   []string
	nReduce int

	mapJobs     []Task
	reduceTasks []Task

	phase   TaskType //分段执行时的当前状态
	allDone bool
}

// Your code here -- RPC handlers for the worker to call.
// Map && Reduce 共用逻辑
func AssignJob(
	tasks []Task,
	reply *GetTaskReply,
	phase TaskType,
) bool {
	for i, item := range tasks {
		if item.taskState == Idle {
			tasks[i].taskState = Processing
			tasks[i].startTime = time.Now()

			reply.TaskType = phase
			reply.TaskID = tasks[i].id
			reply.FileName = tasks[i].fileName

			return true
		}
	}
	return false
}

func CheckJobsDone(
	tasks []Task,
) bool {
	for _, item := range tasks {
		if item.taskState != Complete {
			return false
		}
	}

	return true
}

func (c *Coordinator) GetTask(
	args *GetTaskArgs,
	reply *GetTaskReply,
) error {
	c._mu.Lock()
	defer c._mu.Unlock()

	if c.phase == TaskMap { //分配 Map 任务阶段
		reply.NReduce = c.nReduce
		if AssignJob(c.mapJobs, reply, c.phase) {
			return nil
		}

		if !CheckJobsDone(c.mapJobs) {
			reply.TaskType = TaskWait
			return nil
		}

		c.phase = TaskReduce
	}

	if c.phase == TaskReduce { // 分配 Reduce 任务阶段
		reply.NMap = len(c.mapJobs)
		if AssignJob(c.reduceTasks, reply, c.phase) {
			return nil
		}

		if !CheckJobsDone(c.reduceTasks) {
			reply.TaskType = TaskWait
			return nil
		}
		c.allDone = true
		c.phase = TaskEnd
	}

	if c.phase == TaskEnd || c.allDone {
		reply.TaskType = TaskEnd
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c._mu.Lock()
	defer c._mu.Unlock()

	var CurState = Complete

	log.Printf("Report Type %d Task.%d.\n", args.TaskType, args.TaskID)

	if !args.IsSuccess {
		CurState = Failed
		log.Printf("Task Run Failed.\n")
	}

	switch args.TaskType {
	case TaskMap:
		if c.mapJobs[args.TaskID].taskState == Processing {
			c.mapJobs[args.TaskID].taskState = CurState
			reply.StateChange = true
		}
	case TaskReduce:
		if c.reduceTasks[args.TaskID].taskState == Processing {
			c.reduceTasks[args.TaskID].taskState = CurState
			reply.StateChange = true
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown Task Type.\n")
	}
	return nil
}

func (c *Coordinator) Stop() {
	c._mu.Lock()
	c.allDone = true
	defer c._mu.Unlock()

	if nil != c._cancel {
		c._cancel() //触发 _ctx.Done()
	}
}

// 检测超时
func (c *Coordinator) TaskDetector() {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c._mu.Lock()
			if c.allDone {
				c._mu.Unlock()
				return
			}
		case <-c._ctx.Done(): // <- c._cancel()
			return
		}

		timeout := time.Second * 10 //超时时间

		log.Printf("Tick Timer at state %v.", c.phase)

		if c.phase == TaskMap {
			for i := range c.mapJobs {
				if c.mapJobs[i].taskState == Processing && time.Since(c.mapJobs[i].startTime) > timeout {
					c.mapJobs[i].taskState = Idle // 重置，等待下次分配
				}
			}
		}

		if c.phase == TaskReduce {
			for i := range c.reduceTasks {
				if c.reduceTasks[i].taskState == Processing && time.Since(c.mapJobs[i].startTime) > timeout {
					c.reduceTasks[i].taskState = Idle // 重置，等待下次分配
				}
			}
		}
		c._mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c._mu.Lock()
	defer c._mu.Unlock()

	ret = c.allDone

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapJobs:     make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		allDone:     false,
		phase:       TaskMap,
		_ctx:        ctx,
		_cancel:     cancel,
	}

	// Your code here.
	// 初始化
	for i, file := range files {
		c.mapJobs[i] = Task{
			id:        i,
			taskState: Idle,
			fileName:  file,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			id:        i,
			taskState: Idle,
		}
	}

	c.server()
	return &c
}
