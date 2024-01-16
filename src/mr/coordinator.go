package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	phase             string
	files             []string
	nReduce           int
	mapPhaseStatus    map[string]bool
	reducePhaseStatus map[int]bool
	mutex             sync.Mutex
}

func (c *Coordinator) IsMapPhaseDone() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, file := range c.files {
		if !c.mapPhaseStatus[file] {
			return false
		}
	}
	return true
}

func (c *Coordinator) isReducePhaseDone() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i := 0; i < c.nReduce*len(c.files); i++ {
		if !c.reducePhaseStatus[i] {
			return false
		}
	}
	return true
}

func (c *Coordinator) getMapTaskStatus(key string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapPhaseStatus[key]
}

func (c *Coordinator) getReduceTaskStatus(key int) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.reducePhaseStatus[key]
}

func (c *Coordinator) assignNextMapTask(reply *RequestTaskReply) {
	for i, file := range c.files {
		status := c.getMapTaskStatus(file)
		if !status {
			reply.InputFile = file
			reply.MapTaskIdx = i
			break
		}
	}
}

func (c *Coordinator) assignNextReduceTask(reply *RequestTaskReply) {
	for i := 0; i < c.nReduce; i++ {
		status := c.getReduceTaskStatus(i)
		if !status {
			reply.ReduceTaskIdx = i
			break
		}
	}
}

// rpc for workers to notify coordinator when a task is finished
func (c *Coordinator) TaskIsFinished(args *TaskIsFinishedArgs, reply *TaskIsFinishedReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.phase == "map" {
		c.mapPhaseStatus[args.Filename] = true
	} else if c.phase == "reduce" {
		c.reducePhaseStatus[args.ReduceTask] = true
	}
	return nil
}

// rpc for workers to request a task
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	fmt.Printf("worker requested a task\n")
	if c.phase == "map" {
		fmt.Printf("assigning map task\n")
		c.assignNextMapTask(reply)
		reply.TaskPhase = "map"
	} else if c.phase == "reduce" {
		// start assigning reduce tasks to workers
		fmt.Printf("reduce phase starting\n")
		c.assignNextReduceTask(reply)
		reply.TaskPhase = "reduce"
	}
	reply.NReduce = c.nReduce
	fmt.Printf("request task rpc end\n")
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.IsMapPhaseDone() {
		c.phase = "reduce"
	}
	if c.isReducePhaseDone() {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.mapPhaseStatus = make(map[string]bool)
	c.reducePhaseStatus = make(map[int]bool)
	c.phase = "map"
	c.server()
	return &c
}
