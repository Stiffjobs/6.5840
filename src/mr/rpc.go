package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

// NOTE: Asking for task from coordinator
type MessageArgs struct {
	TaskID              int
	TaskCompletedStatus TaskCompletedStatus
}

type ExampleReply struct {
	Y int
}

type MessageReply struct {
	TaskID   int
	Filename string
	NReduce  int
	NMap     int
	TaskType TaskType
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
