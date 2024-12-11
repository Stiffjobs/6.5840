package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Mu             sync.Mutex
	NReduce        int
	NMap           int
	MapTaskList    []TaskInfo
	ReduceTaskList []TaskInfo
	Files          []string

	AllMapsCompleted    bool
	AllReducesCompleted bool
}

type TaskCompletedStatus int

const (
	MapTaskCompleted = iota
	MapTaskFailed
	ReduceTaskCompleted
	ReduceTaskFailed
)

type TaskType int

const (
	MapTask = iota
	ReduceTask
	Wait
	Exit
)

type TaskStatus int

const (
	Unassigned = iota
	Assigned
	Completed
	Failed
)

type TaskInfo struct {
	ID        int
	Filename  string
	Status    TaskStatus
	Type      TaskType
	StartedAt time.Time
}

// Your code here -- RPC handlers for the worker to call.

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

	// Your code here.

	return c.AllMapsCompleted && c.AllReducesCompleted
}

func (c *Coordinator) RequestTask(args *MessageArgs, reply *MessageReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	// Handle completed task status if any
	if !c.AllMapsCompleted {
		MapCompleted := 0
		for idx, taskInfo := range c.MapTaskList {
			if taskInfo.Status == Unassigned || taskInfo.Status == Failed || taskInfo.Status == Assigned && time.Since(taskInfo.StartedAt) > 10*time.Second {
				reply.Filename = taskInfo.Filename
				reply.TaskID = idx
				reply.NReduce = c.NReduce
				reply.TaskType = MapTask

				c.MapTaskList[idx].Status = Assigned
				c.MapTaskList[idx].StartedAt = time.Now()

				return nil

			} else if taskInfo.Status == Completed {
				MapCompleted++
			}
		}

		if MapCompleted == len(c.MapTaskList) {
			c.AllMapsCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	if !c.AllReducesCompleted {
		reduceCompleted := 0
		for idx, taskInfo := range c.ReduceTaskList {

			if taskInfo.Status == Unassigned || taskInfo.Status == Failed || taskInfo.Status == Assigned && time.Since(taskInfo.StartedAt) > 10*time.Second {
				reply.TaskID = idx
				reply.TaskType = ReduceTask
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap

				c.ReduceTaskList[idx].Status = Assigned
				c.ReduceTaskList[idx].StartedAt = time.Now()
				return nil
			} else if taskInfo.Status == Completed {
				reduceCompleted++
			}

		}
		if reduceCompleted == len(c.ReduceTaskList) {
			c.AllReducesCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	// Rest of your task assignment logic here...
	reply.TaskType = Exit
	return nil
}

func (c *Coordinator) ReportTask(args *MessageArgs, reply *MessageReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	switch args.TaskCompletedStatus {
	case MapTaskFailed:
		c.MapTaskList[args.TaskID].Status = Failed
		return nil
	case MapTaskCompleted:
		c.MapTaskList[args.TaskID].Status = Completed
		return nil
	case ReduceTaskCompleted:
		c.ReduceTaskList[args.TaskID].Status = Completed
		return nil
	case ReduceTaskFailed:
		c.ReduceTaskList[args.TaskID].Status = Failed
		return nil
	}

	return nil
}

func (c *Coordinator) Init() {
	for index, f := range c.Files {
		c.MapTaskList = append(c.MapTaskList, TaskInfo{
			Filename: f,
			ID:       index,
			Status:   Unassigned,
			Type:     MapTask,
		})
	}

	for i := 0; i < c.NReduce; i++ {
		c.ReduceTaskList = append(c.ReduceTaskList, TaskInfo{
			ID:     i,
			Status: Unassigned,
			Type:   ReduceTask,
		})
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:                len(files),
		NReduce:             nReduce,
		MapTaskList:         []TaskInfo{},
		ReduceTaskList:      []TaskInfo{},
		AllMapsCompleted:    false,
		AllReducesCompleted: false,
		Files:               files,
		Mu:                  sync.Mutex{},
	}

	// Your code here.

	c.Init()
	c.server()
	return &c
}
