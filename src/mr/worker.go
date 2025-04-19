package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

	for {
		reply, ok := requestTask()
		if !ok {
			break
		}
		switch reply.TaskType {
		case Map:
			mapTask(mapf, reply)
		case Reduce:
			reduceTask(reducef, reply.TaskNumber)
		}
	}
}

func requestTask() (GetTaskReply, bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

func mapTask(mapf func(string, string) []KeyValue, reply GetTaskReply) error {
	content := getContent(reply.Filename)

	kva := mapf(reply.Filename, string(content))

	var encoders []*json.Encoder
	for i := range reply.NReduce {
		oname := fmt.Sprintf("mr-%d-%d.json", reply.TaskNumber, i)

		ofile, err := os.Create(oname)
		if err != nil {
			return err
		}
		defer ofile.Close()

		encoder := json.NewEncoder(ofile)
		encoders = append(encoders, encoder)
	}

	for _, kv := range kva {
		reduceNumber := ihash(kv.Key) % reply.NReduce

		err := encoders[reduceNumber].Encode(kv)
		if err != nil {
			return err
		}
	}

	callMappingCompleted(reply.TaskNumber)
	return nil
}

func callMappingCompleted(mappingNumber int) {
	args := CompletedArgs{
		Number: mappingNumber,
	}
	ok := call("Coordinator.MappingCompleted", &args, nil)
	if !ok {
		fmt.Printf("call Coordinator.MappingCompleted failed!\n")
	}
}

func getContent(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func reduceTask(reducef func(string, []string) string, reduceNumber int) error {
	filenames, err := filesToReduce(reduceNumber)
	if err != nil {
		return err
	}

	var kva []KeyValue

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	values := make(map[string][]string)
	for _, kv := range kva {
		values[kv.Key] = append(values[kv.Key], kv.Value)
	}

	oname := fmt.Sprintf("mr-out-%d", reduceNumber)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	for key, value := range values {
		output := reducef(key, value)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	callReducingCompleted(reduceNumber)
	return nil
}

func filesToReduce(reduceNumber int) ([]string, error) {
	pattern := fmt.Sprintf("mr-*-%d.json", reduceNumber)
	filenames, err := filepath.Glob(pattern)
	return filenames, err
}

func callReducingCompleted(reduceNumber int) {
	args := CompletedArgs{
		Number: reduceNumber,
	}
	ok := call("Coordinator.ReducingCompleted", &args, nil)
	if !ok {
		fmt.Printf("call Coordinator.ReducingCompleted failed!\n")
	}
}
