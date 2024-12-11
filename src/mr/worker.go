package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := MessageArgs{}
		reply := MessageReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			break
		}

		switch reply.TaskType {
		case MapTask:
			HandleMap(mapf, &reply)
		case ReduceTask:
			HandleReduce(reducef, &reply)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(time.Second)
		}
	}
}

func HandleMap(mapf func(string, string) []KeyValue, reply *MessageReply) {
	intermediate := make([][]KeyValue, reply.NReduce)

	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	kva := mapf(reply.Filename, string(content))

	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	for r, kva := range intermediate {
		filename := fmt.Sprintf("mr-%v-%v", reply.TaskID, r)
		ifile, err := os.CreateTemp("", filename)

		if err != nil {
			log.Fatal()
		}

		enc := json.NewEncoder(ifile)

		for _, v := range kva {
			enc.Encode(v)
		}

		ifile.Close()
		//NOTE: Atomic file renaming
		os.Rename(ifile.Name(), filename)
	}

	call("Coordinator.ReportTask", &MessageArgs{TaskID: reply.TaskID, TaskCompletedStatus: MapTaskCompleted}, &MessageReply{})
}

func getFilenames(r int, NMap int) []string {
	filenames := []string{}
	for mapTaskId := 0; mapTaskId < NMap; mapTaskId++ {
		filenames = append(filenames, fmt.Sprintf("mr-%v-%v", mapTaskId, r))
	}

	return filenames
}
func HandleReduce(reducef func(string, []string) string, reply *MessageReply) {
	intermediates := getFilenames(reply.TaskID, reply.NMap)

	var intermediate []KeyValue
	for _, filename := range intermediates {
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}

		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err == io.EOF {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	//NOTE: sort the intermediate by keys
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oFileName := fmt.Sprintf("mr-out-%v", reply.TaskID)
	ofile, err := os.Create(oFileName)
	if err != nil {
		log.Fatalf("cannot create %v", oFileName)
		return
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()

	os.Rename(ofile.Name(), oFileName)

	args := &MessageArgs{
		TaskID:              reply.TaskID,
		TaskCompletedStatus: ReduceTaskCompleted,
	}

	call("Coordinator.ReportTask", args, &MessageReply{})
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
