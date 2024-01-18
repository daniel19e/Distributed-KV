package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	notAssigned TaskState = 0
	assigned              = 1
	completed             = 2
)

type Coordinator struct {
	files                []string
	nReduce              int
	mapPhaseStatus       map[string]TaskState
	mapTaskAssignTime    map[string]time.Time
	reducePhaseStatus    map[int]TaskState
	reduceTaskAssignTime map[int]time.Time
	mapLock              sync.Mutex
	taskLock             sync.Mutex
}

func (c *Coordinator) isMapPhaseDone() bool {
	for _, file := range c.files {
		if c.mapPhaseStatus[file] != completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) isReducePhaseDone() bool {
	for i := 0; i < c.nReduce; i++ {
		if c.reducePhaseStatus[i] != completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) getMapTaskStatus(key string) TaskState {
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	return c.mapPhaseStatus[key]
}

func (c *Coordinator) getReduceTaskStatus(key int) TaskState {
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	return c.reducePhaseStatus[key]
}

func (c *Coordinator) setMapTaskToAssigned(file string) {
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	c.mapPhaseStatus[file] = assigned
	c.mapTaskAssignTime[file] = time.Now()
}

func (c *Coordinator) setReduceTaskToAssigned(task int) {
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	c.reducePhaseStatus[task] = assigned
	c.reduceTaskAssignTime[task] = time.Now()

}

func (c *Coordinator) assignNextMapTask(reply *RequestTaskReply) {
	for i, file := range c.files {
		status := c.getMapTaskStatus(file)
		if status == notAssigned {
			reply.InputFile = file
			reply.MapTaskIdx = i
			c.setMapTaskToAssigned(file)
			break
		}
	}
}

func (c *Coordinator) assignNextReduceTask(reply *RequestTaskReply) {
	for i := 0; i < c.nReduce; i++ {
		status := c.getReduceTaskStatus(i)
		if status == notAssigned {
			reply.ReduceTaskIdx = i
			c.setReduceTaskToAssigned(i)
			break
		}
	}
}

// rpc for workers to notify coordinator when a task is finished
func (c *Coordinator) TaskIsFinished(args *TaskIsFinishedArgs, reply *TaskIsFinishedReply) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if args.TaskPhase == "map" {
		c.mapPhaseStatus[args.Filename] = completed
	} else if args.TaskPhase == "reduce" {
		c.reducePhaseStatus[args.ReduceTask] = completed
	}
	return nil
}

// rpc for workers to request a task
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//fmt.Printf("worker requested a task\n")
	//fmt.Printf("map map status %v\n", c.mapPhaseStatus)
	//fmt.Printf("reduce map status %v\n", c.reducePhaseStatus)
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	reply.MapTaskIdx = -1
	reply.ReduceTaskIdx = -1
	isMapDone := c.isMapPhaseDone()
	if !isMapDone {
		c.assignNextMapTask(reply)
	} else {
		c.assignNextReduceTask(reply)
	}
	reply.NReduce = c.nReduce
	reply.NumFiles = len(c.files)
	//fmt.Printf("request task rpc end\n")
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
	//fmt.Printf("calling done\n")
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if c.isMapPhaseDone() && c.isReducePhaseDone() {
		fmt.Printf("all jobs are done\n")
		return true
	}
	return false
}

func (c *Coordinator) checkTaskTimeouts() {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	now := time.Now()
	timeoutDuration := 10 * time.Second

	for file, assignTime := range c.mapTaskAssignTime {
		if c.mapPhaseStatus[file] == assigned && now.Sub(assignTime) > timeoutDuration {
			c.mapPhaseStatus[file] = notAssigned
		}
	}

	for task, assignTime := range c.reduceTaskAssignTime {
		if c.reducePhaseStatus[task] == assigned && now.Sub(assignTime) > timeoutDuration {
			c.reducePhaseStatus[task] = notAssigned
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce

	c.mapPhaseStatus = make(map[string]TaskState)
	for _, file := range files {
		c.mapPhaseStatus[file] = notAssigned
	}
	c.reducePhaseStatus = make(map[int]TaskState)

	for i := 0; i < nReduce; i++ {
		c.reducePhaseStatus[i] = notAssigned
	}
	c.mapTaskAssignTime = make(map[string]time.Time)
	c.reduceTaskAssignTime = make(map[int]time.Time)

	c.server()

	go func() {
		checkInterval := 1 * time.Second
		for {
			time.Sleep(checkInterval)
			c.checkTaskTimeouts()
		}
	}()

	return &c
}
