package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := requestTask()
	mapTask(mapf, reply)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func requestTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.MapNumber %v; reply.Filename %s, reply.NReduce %v\n", reply.MapNumber, reply.Filename, reply.NReduce)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func mapTask(mapf func(string, string) []KeyValue, reply GetTaskReply) error {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	kva := mapf(reply.Filename, string(content))

	var ofiles []*os.File
	for i := range reply.NReduce {
		oname := fmt.Sprintf("mr-out-%d-%d", reply.MapNumber, i)
		ofile, err := os.Create(oname)

		if err != nil {
			return err
		}
		defer ofile.Close()

		ofiles = append(ofiles, ofile)
	}

	for _, kv := range kva {
		reduceNumber := ihash(kv.Key) % reply.NReduce
		_, err := fmt.Fprintf(ofiles[reduceNumber], "%s %s\n", kv.Key, kv.Value)

		if err != nil {
			return err
		}
	}
	return nil
}
