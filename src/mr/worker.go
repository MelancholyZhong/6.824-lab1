package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	reply := AskForWork(-1, "")
	for reply.Step != "finished"{
		finished := -1
		if reply.FileID != -1{
			if reply.Step == "map"{
				finished = mapWork(reply, mapf)
			}else{
				finished = reduceWork(reply, reducef)
			}
		}
	}

}

func AskForWork(finishedWork int, step string ) AskReply {
	args := AskArgs{}
	args.Finished = finishedWork
	
	reply := AskReply{}

	ok := call("Coordinator.AssignJob", &args, &reply)

	if ok && reply.FileID != -1 {
		fmt.Printf("reply.filename %v\n", reply.Filename)
	}else if ok {
		fmt.Printf("Currently no work\n")
	}else{
		fmt.Printf("call failed!\n")
	}

	return reply
}

func mapWork(reply AskReply, mapf func(string, string) []KeyValue) int {
	filename := reply.Filename
	nReduce := reply.NReduce
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
	ra := []ByKey{}
	for i:=0; i<nReduce;i++{
		reduce := ByKey{}
		ra = append(ra, reduce)
	}
	for _,kv := range kva{
		ra[ihash(kv.Key)%reply.NReduce] = append(ra[ihash(kv.Key)%reply.NReduce], kv)
	}
	for i,reduce := range ra {
		sort.Sort(ra[i])
		intermediateFile, err := os.Create("mr-"+strconv.Itoa(reply.FileID)+"-"+strconv.Itoa(i)+".json")
		if err != nil {
			log.Fatalf("cannot create new file")
		}
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range reduce {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write kv into json")
			}
		}
		intermediateFile.Close()
	}
	return reply.FileID
}

func reduceWork(reply AskReply, reducef func(string, []string) string) int {
	
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
