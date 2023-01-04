package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	files []string
	nReduce int
	mapMonitor []workStatus
	reduceMonitor []workStatus
	mu sync.Mutex
}

type workStatus struct {
	workName string
	status int
	worker string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignJob(args *AskArgs, reply *AskReply) error {
	for i,work := range c.mapMonitor{
		if work.status == -1 {
			
			c.mapMonitor[i].status = 0
			c.mapMonitor[i].worker = args.WorkerID
			reply.Filename = c.mapMonitor[i].workName
			break
		}
	}

	
	return nil
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
	c.mapMonitor = []workStatus{}
	for _, file := range files{
		status := workStatus{workName: file, status: -1, worker: ""} 
		c.mapMonitor = append(c.mapMonitor, status)
	}
	
	// Your code here.

	c.server()
	return &c
}
