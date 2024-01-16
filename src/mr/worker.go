package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeKeyValueToFile(filename string, kva []KeyValue) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			return err
		}
	}
	return nil
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
func handleMapPhase(filename string, nReduce int, mapTaskIdx int, mapf func(string, string) []KeyValue) {
	fmt.Printf("filename %v\n", filename)
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
	buckets := make([][]KeyValue, nReduce)

	for i := range buckets {
		buckets[i] = []KeyValue{}
	}

	for _, kv := range kva {
		reduceTaskIdx := ihash(kv.Key) % nReduce
		buckets[reduceTaskIdx] = append(buckets[reduceTaskIdx], kv)
	}

	for reduceTaskIdx, bucket := range buckets {
		if len(bucket) > 0 {
			intermediateFile := fmt.Sprintf("mr-%d-%d", mapTaskIdx, reduceTaskIdx)
			fmt.Printf("Intermediate file: %v\n", intermediateFile)
			err := writeKeyValueToFile(intermediateFile, bucket)
			if err != nil {
				log.Fatalf("Failed to write to intermediate file: %v", err)
			}
		}
	}
	MapTaskIsFinished(filename)
	fmt.Printf("map completed\n")
	time.Sleep(1 * time.Second)
}

func readIntermediateFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filename, err)
	}
	defer file.Close()

	var kvs []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to decode JSON from %s: %v", filename, err)
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}

func handleReducePhase(reducef func(string, []string) string, reduceTaskIdx int, numFiles int) {
	oname := fmt.Sprintf("mr-out-%v", reduceTaskIdx)
	outputFile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()
	intermediate := []KeyValue{}
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskIdx)
		kvPairs, err := readIntermediateFile(filename)
		if err != nil {
			log.Printf("Failed to read intermediate file %s: %v", filename, err)
			continue
		}
		intermediate = append(intermediate, kvPairs...)
	}
	// aggregate keys and apply reduce function
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ReduceTaskIsFinished(reduceTaskIdx)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		phase, filename, mapTaskIdx, reduceTaskIdx, nReduce, err := CallRequestTask()
		fmt.Printf("%v %v %v %v\n", filename, phase, mapTaskIdx, nReduce)
		if err != nil {
			fmt.Printf("No file to open\n")
			break
		}
		if phase == "map" {
			handleMapPhase(filename, nReduce, mapTaskIdx, mapf)
		} else if phase == "reduce" {
			handleReducePhase(reducef, reduceTaskIdx, 8)
		}
	}
}

// RPC to notify coordinator a map task was finished
func MapTaskIsFinished(key string) {
	fmt.Printf("task is finished rpc\n")
	args := TaskIsFinishedArgs{}
	args.Filename = key
	args.TaskPhase = "map"
	reply := TaskIsFinishedReply{}

	ok := call("Coordinator.TaskIsFinished", &args, &reply)
	if ok {

	} else {
	}
}

// RPC to notify coordinator a reduce task is finished
func ReduceTaskIsFinished(reduceIdx int) {
	fmt.Printf("task is finished rpc\n")
	args := TaskIsFinishedArgs{}
	args.TaskPhase = "reduce"
	args.ReduceTask = reduceIdx
	reply := TaskIsFinishedReply{}

	ok := call("Coordinator.TaskIsFinished", &args, &reply)
	if ok {

	} else {
	}
}

// request task RPC
func CallRequestTask() (string, string, int, int, int, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("task requested\n")
		return reply.TaskPhase, reply.InputFile, reply.MapTaskIdx, reply.ReduceTaskIdx, reply.NReduce, nil
	} else {
		errorMsg := "request task call failed!\n"
		fmt.Printf(errorMsg)
		return "", "", -1, -1, -1, errors.New(errorMsg)
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
