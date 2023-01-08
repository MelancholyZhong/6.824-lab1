package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	files []string
	nReduce int
	mapMonitor []workStatus
	reduceMonitor []workStatus
	step string
	mu sync.Mutex
}

type workStatus struct {
	workName string
	status int
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.


func (c *Coordinator) AssignJob(args *AskArgs, reply *AskReply) error {
	c.finishJob(args)
	c.findJob(reply)
	return nil
}

func (c *Coordinator) findJob(reply *AskReply){
	c.mu.Lock()
	var monitor []workStatus
	if c.step == "map" {
		monitor = c.mapMonitor
	}else if c.step == "reduce" {
		monitor = c.reduceMonitor
	}else{
		reply.FileID = -1
		reply.Step = c.step
		c.mu.Unlock()
		return
	}
	currentStepFinished := true
	for i, status := range monitor {
		if status.status != 1{
			currentStepFinished = false
			if status.status == -1{
				reply.FileID = i
				reply.Step = c.step
				reply.Filename = status.workName
				reply.NReduce = c.nReduce
				status.status = 0
				status.startTime = time.Now()
				c.mu.Unlock()
				return
			}else{
				currentTime := time.Now()
				if currentTime.Sub(status.startTime) > 1000{
					reply.FileID = i
					reply.Step = c.step
					reply.Filename = status.workName
					reply.NReduce = c.nReduce
					status.startTime = time.Now()
					c.mu.Unlock()
					return
				}
			}
		}
	}
	if currentStepFinished {
		if c.step == "map"{
			c.step = "reduce"
		}else{
			c.step = "finished"
		}
	}
	reply.FileID = -1
	reply.Step = c.step
	c.mu.Unlock()
}

func (c *Coordinator) finishJob(args *AskArgs){
	workname := args.Finished
	step := args.Step
	if workname == -1 || step!=c.step {return}
	var monitor []workStatus
	if c.step == "map" {
		monitor = c.mapMonitor
	}else if c.step == "reduce" {
		monitor = c.reduceMonitor
	}else{
		return
	}
	c.mu.Lock()
	if monitor[workname].status != 1{
		monitor[workname].status = 1
	}
	c.mu.Unlock()
	return
}



//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.step = "map"

	c.mapMonitor = []workStatus{}
	for _, file := range files{
		status := workStatus{workName: file, status: -1} 
		c.mapMonitor = append(c.mapMonitor, status)
	}

	c.reduceMonitor = []workStatus{}
	for i:=1; i< nReduce; i++{
		status := workStatus{workName: strconv.Itoa(i), status: -1} 
		c.reduceMonitor = append(c.reduceMonitor, status)
	}
	// Your code here.

	c.server()
	return &c
}
