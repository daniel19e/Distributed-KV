package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type RequestTaskArgs struct {
	Filename string
}
type RequestTaskReply struct {
	InputFile     string
	TaskPhase     string
	ReduceTaskIdx int
	MapTaskIdx    int
	NReduce       int
}

type TaskIsFinishedArgs struct {
	Filename   string
	TaskPhase  string
	ReduceTask int
}
type TaskIsFinishedReply struct {
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
