package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu     sync.Mutex
	data   map[string]string
	exists map[int64]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Type == Committed {
		delete(kv.exists, args.ID)
		return
	}

	//INFO: If already exists, do nothing
	if val, ok := kv.exists[args.ID]; ok {
		reply.Value = val
		return
	}

	reply.Value = kv.data[args.Key]
	kv.exists[args.ID] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Type == Committed {
		delete(kv.exists, args.ID)
		return
	}
	if _, ok := kv.exists[args.ID]; ok {
		//INFO: If already exists, do nothing
		return
	}
	kv.data[args.Key] = args.Value
	kv.exists[args.ID] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Type == Committed {
		delete(kv.exists, args.ID)
		return
	}

	if val, ok := kv.exists[args.ID]; ok {
		reply.Value = val
		return
	}

	old := kv.data[args.Key]
	kv.data[args.Key] += args.Value
	reply.Value = old
	//append to old value, return old value
	kv.exists[args.ID] = old
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.exists = make(map[int64]string)
	return kv
}
