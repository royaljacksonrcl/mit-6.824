package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type CacheFile struct {
	fileHandle  *os.File
	jsonEncoder *json.Encoder
}

type KVList []KeyValue

func (list KVList) Len() int           { return len(list) }
func (list KVList) Swap(i, j int)      { list[i], list[j] = list[j], list[i] }
func (list KVList) Less(i, j int) bool { return list[i].Key < list[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// 请求任务
		req := GetTaskArgs{}
		rsp := GetTaskReply{}

		if !call("Coordinator.GetTask", &req, &rsp) {
			fmt.Fprintf(os.Stderr, "Call Coordinator Failed.\n")
			return
		}

		log.Printf("Worker Running...Got Task.%d", rsp.TaskID)

		// 处理任务
		switch rsp.TaskType {
		case TaskMap:
			handleMapTask(rsp, mapf)
		case TaskReduce:
			handleReduceTask(rsp, reducef)
		case TaskWait:
			time.Sleep(time.Millisecond * 200)
		case TaskEnd: // 处理完成后等待完成消息跳出
			return
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// 产生临时文件 文件名规则为 mr-MapID-ReduceID
func handleMapTask(maptask GetTaskReply, mapf func(string, string) []KeyValue) {
	log.Printf("Dealing Map Task.\n")
	// 读取源文件
	ret := false
	data, err := os.ReadFile(maptask.FileName)
	if nil != err {
		log.Printf("Cannot read file %v", maptask.FileName)
	}

	list := mapf(maptask.FileName, string(data))

	// 创建临时文件，收集需要分配的数据内容
	file_list := make([]*CacheFile, maptask.NReduce)

	for i := 0; i < maptask.NReduce; i++ {
		//初始化
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-map-tmp-%d-%d-*", maptask.TaskID, i))
		if err != nil {
			log.Fatal(err)
		}

		file_list[i] = &CacheFile{
			fileHandle:  tempFile,
			jsonEncoder: json.NewEncoder(tempFile),
		}
	}

	// 分配任务给 reduceMap 通过report汇报给 coordinator
	for _, kv := range list {
		// 临时文件
		idx := ihash(kv.Key) % maptask.NReduce
		err := file_list[idx].jsonEncoder.Encode(&kv)
		if nil != err {
			log.Fatal(err)
		}
	}

	// 关闭临时文件
	for i, file := range file_list {
		file.fileHandle.Close()
		final_name := fmt.Sprintf("rm-%d-%d", maptask.TaskID, i)
		os.Rename(file.fileHandle.Name(), final_name)
		ret = true
	}

	ReportArgs := ReportArgs{
		TaskType:  TaskMap,
		TaskID:    maptask.TaskID,
		IsSuccess: ret,
	}
	ReportReply := ReportReply{}

	call("Coordinator.ReportTask", &ReportArgs, &ReportReply)
}

func handleReduceTask(reducetask GetTaskReply, reducef func(string, []string) string) {
	log.Printf("Dealing Reduce Task.\n")
	intermediate := []KeyValue{}

	for i := 0; i < reducetask.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reducetask.TaskID)
		filehandle, err := os.Open(filename)
		if nil != err {
			// 文件不存在表示 Map 处理流程异常
			log.Printf("File %v do not exist.", filename)
			continue
		}

		dec := json.NewDecoder(filehandle)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); nil != err {
				break
			}
			intermediate = append(intermediate, kv)
		}
		filehandle.Close()
	}

	//排序
	sort.Sort(KVList(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reducetask.TaskID)
	ofile, err := os.CreateTemp("", "mr-reduce-*")
	if nil != err {
		log.Fatal(err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)

	// Report
	reportArgs := ReportArgs{
		TaskType:  TaskReduce,
		TaskID:    reducetask.TaskID,
		IsSuccess: true,
	}
	reportReply := ReportReply{}
	call("Coordinator.ReportTask", &reportArgs, &reportReply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
