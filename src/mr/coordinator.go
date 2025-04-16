package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Status int

const (
	NotAssigned Status = iota
	Assigned
	Completed
)

type Coordinator struct {
	filenames   []string
	mapped      []Status
	reduced     []Status
	nReduce     int
	mappedLock  sync.Mutex
	reducedLock sync.Mutex
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	// ret := true

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filenames: files,
		mapped:    make([]Status, len(files)),
		reduced:   make([]Status, nReduce),
		nReduce:   nReduce,
	}

	c.server()
	return &c
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mappedLock.Lock()
	for i, v := range c.mapped {
		if v == NotAssigned {
			c.mapped[i] = Assigned
			reply.TaskType = Map
			reply.TaskNumber = i
			reply.Filename = c.filenames[i]
			reply.NReduce = c.nReduce

			c.mappedLock.Unlock()
			return nil
		}
	}
	c.mappedLock.Unlock()

	c.reducedLock.Lock()
	for i, v := range c.reduced {
		if v == NotAssigned {
			c.reduced[i] = Assigned
			reply.TaskType = Reduce
			reply.TaskNumber = i

			c.reducedLock.Unlock()
			return nil
		}
	}
	c.reducedLock.Unlock()
	return errors.New("no available map tasks")
}

func (c *Coordinator) MappingCompleted(args *CompletedArgs, reply *CompletedReply) error {
	c.mappedLock.Lock()
	c.mapped[args.Number] = Completed
	c.mappedLock.Unlock()
	fmt.Printf("Maping %d Completed\n", args.Number)
	return nil
}

func (c *Coordinator) ReducingCompleted(args *CompletedArgs, reply *CompletedReply) error {
	c.reducedLock.Lock()
	c.reduced[args.Number] = Completed
	c.reducedLock.Unlock()
	fmt.Printf("Reducing %d Completed\n", args.Number)
	return nil
}
