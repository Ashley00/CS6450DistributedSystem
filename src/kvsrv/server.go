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
	mu sync.Mutex
	// Your definitions here.
	kvmap map[string]string // In memory kv pairs
	tempmap map[int64]string // Temp map in case of lost messages
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	opnum := args.Opnum

	if args.Success {
		delete(kv.tempmap, opnum)
		kv.mu.Unlock()
		return
	}

	value, ok := kv.tempmap[opnum]
	
	if !ok { // if this operation hasn't executed
		elem, _ := kv.kvmap[args.Key]
		reply.Value = elem
		kv.tempmap[opnum] = elem
	} else {
		reply.Value = value
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	opnum := args.Opnum

	if args.Success {
		delete(kv.tempmap, opnum)
		kv.mu.Unlock()
		return
	}

	value, ok := kv.tempmap[opnum]

	if !ok {
		kv.kvmap[args.Key] = args.Value
		reply.Value = args.Value
		kv.tempmap[opnum] = args.Value
	} else {
		reply.Value = value
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	opnum := args.Opnum

	if args.Success {
		delete(kv.tempmap, opnum)
		kv.mu.Unlock()
		return 
	}

	value, ok := kv.tempmap[opnum]

	if !ok {
		elem, _ := kv.kvmap[args.Key]
		kv.kvmap[args.Key] = elem + args.Value
		reply.Value = elem // return the old value
		kv.tempmap[opnum] = elem
	} else {
		reply.Value = value
	}
	
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvmap = make(map[string]string)
	kv.tempmap = make(map[int64]string)

	return kv
}
