package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	// "fmt"
)

const TaskTimeout = 10

type TaskStatus int

const (
	NotAssigned TaskStatus = iota
	Assigned
	Completed
)

type TaskState struct {
	status TaskStatus
	time   int64
}

type Coordinator struct {
	filenames   []string
	mapped      []TaskState
	reduced     []TaskState
	nReduce     int
	mappedLock  sync.Mutex
	reducedLock sync.Mutex
}

func (s TaskState) NeedsAssignment(now int64) bool {
	return s.status == NotAssigned || (s.status == Assigned && now-s.time > TaskTimeout)
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
	if !c.MappingDone() {
		return false
	}
	if !c.ReducingDone() {
		return false
	}
	return true
}

func (c *Coordinator) ReducingDone() bool {
	c.reducedLock.Lock()
	for _, state := range c.reduced {
		if state.status != Completed {
			c.reducedLock.Unlock()
			return false
		}
	}
	c.reducedLock.Unlock()
	return true
}

func (c *Coordinator) MappingDone() bool {
	c.mappedLock.Lock()
	for _, state := range c.mapped {
		if state.status != Completed {
			c.mappedLock.Unlock()
			return false
		}
	}
	c.mappedLock.Unlock()
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filenames: files,
		mapped:    make([]TaskState, len(files)),
		reduced:   make([]TaskState, nReduce),
		nReduce:   nReduce,
	}

	c.server()
	return &c
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	now := time.Now().Unix()

	c.mappedLock.Lock()
	for i, state := range c.mapped {
		if state.NeedsAssignment(now) {
			c.mapped[i].status = Assigned
			c.mapped[i].time = time.Now().Unix()
			reply.TaskType = Map
			reply.TaskNumber = i
			reply.Filename = c.filenames[i]
			reply.NReduce = c.nReduce

			c.mappedLock.Unlock()
			return nil
		}
	}
	c.mappedLock.Unlock()

	if !c.MappingDone() {
		return nil
	}

	c.reducedLock.Lock()
	for i, state := range c.reduced {
		if state.NeedsAssignment(now) {
			c.reduced[i].status = Assigned
			c.reduced[i].time = time.Now().Unix()
			reply.TaskType = Reduce
			reply.TaskNumber = i

			c.reducedLock.Unlock()
			return nil
		}
	}
	c.reducedLock.Unlock()
	return nil
}

func (c *Coordinator) MappingCompleted(args *CompletedArgs, reply *CompletedReply) error {
	c.mappedLock.Lock()
	c.mapped[args.Number].status = Completed
	c.mappedLock.Unlock()
	// fmt.Printf("Maping %d Completed\n", args.Number)
	return nil
}

func (c *Coordinator) ReducingCompleted(args *CompletedArgs, reply *CompletedReply) error {
	c.reducedLock.Lock()
	c.reduced[args.Number].status = Completed
	c.reducedLock.Unlock()
	// fmt.Printf("Reducing %d Completed\n", args.Number)
	return nil
}
