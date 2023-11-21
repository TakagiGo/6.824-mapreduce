package _Web_Crawler

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.

func calculateHash(key string, nReduceNumber int) int {
	return ihash(key) % nReduceNumber
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
var workID uuid.UUID = uuid.New()
var maxTryTimes = 3
var waitInterval = time.Duration(3)

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	retry := 0
	for {
	call:
		fetchArgs := FetchArgs{
			nodeId: workID,
			msg:    fmt.Sprintf("[worker]Fetch a request"),
		}
		fetchReply := FetchReply{}

		ok := call("Coordinator.FetchTask", &fetchArgs, &fetchReply)
		if ok {
			// reply.Y should be 100.
			task := fetchReply.task
			switch task.taskType {
			case MapTask:
				log.Printf("[worker]Fetch a map task")
				processMapTask(mapf, task)
			case ReduceTask:
				log.Printf("[worker]Fetch a reduce task")
				processReduceTask(task, reducef)
			case WaitTask:
				log.Printf("[worker]Fetch a Wait task")
				time.Sleep(waitInterval * time.Second)
			case ExitTask:
				log.Printf("[worker]Fetch a Wait task, exit gracefully")
				return
			}
		} else {
			if retry+1 == maxTryTimes {
				goto fail
			}
			goto call
		}
	fail:
		log.Printf("[worker]node %d has ask for request 3 times,exit.", workID)
		return
	}

}

func processMapTask(mapf func(string, string) []KeyValue, task *Task) {
	filename := task.files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	var intermediates = make([][]KeyValue, task.nReduceNumber)
	for i := 0; i < task.nReduceNumber; i++ {
		intermediates[i] = []KeyValue{}
	}
	for _, kv := range kva {
		index := calculateHash(kv.Key, task.nReduceNumber)
		intermediates[index] = append(intermediates[index], kv)
	}
	for i, intermediate := range intermediates {
		tmpFile, err := ioutil.TempFile(".", "mrtmp-map")
		if err != nil {
			log.Panic("[worker]can't create a tempFile")
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range intermediate {
			enc.Encode(kv)
		}
		outName := fmt.Sprintf(IntermediateNameTemplate, task.taskId, i)
		err = os.Rename(tmpFile.Name(), outName)
		if err != nil {
			log.Panic("[worker]can't rename a tempFile")
		}
	}

}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var oname = "mr-out-%d"

func processReduceTask(task *Task, reducef func(string, []string) string) {
	fileNames := task.files
	var intermediate = []KeyValue{}
	for _, filename := range fileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	file := fmt.Sprintf(oname, task.taskId)
	ofile, _ := os.Create(file)
	sort.Sort(ByKey(intermediate))
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

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
