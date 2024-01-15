package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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

func writeJSON(file string) {
	//enc := json.NewEncoder(file)
	//for _, kv := ... {
	//  err := enc.Encode(&kv)
}
func readJSON(file string) {
	//dec := json.NewDecoder(file)
	//for {
	//  var kv KeyValue
	//  if err := dec.Decode(&kv); err != nil {
	//	break
	//  }
	//  kva = append(kva, kv)
	//}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	filename, err := CallRequestTask()
	if err != nil {
		log.Fatalf("Rpc error in CallRequestTask %v", filename)
	}
	fmt.Printf("fileToProcess %v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error: opening file %v", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Error getting file content for %v", file)
	}
	file.Close()
	kva := mapf(filename, string(content))
	fmt.Printf("kva %v\n", kva)
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

// request task RPC
func CallRequestTask() (string, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("task requested\n")
		return reply.FileToProcess, nil
	} else {
		errorMsg := "request task call failed!\n"
		fmt.Printf(errorMsg)
		return "", errors.New(errorMsg)
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
