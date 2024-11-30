package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Operation string
	Id int64
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandMap map[int64]string
	kvMap map[string]string
	waitChan map[int]chan *Op
	timeout time.Duration
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//kv.mu.Lock()
	//log.Println("Get")
	opnum := args.Opnum
	op := Op{Key: args.Key, Operation: "Get", Id: opnum}
	// value, ok := kv.commandMap[opnum]

	// if ok {
	// 	reply.Err = OK
	// 	reply.Value = value
	// 	kv.mu.Unlock()
	// 	return
	// }
	//kv.mu.Unlock()

	// if this operation hasn't executed
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	waitChan, ok := kv.waitChan[index]
	if !ok { // check if waitChan has message
		kv.waitChan[index] = make(chan *Op, 1)
		waitChan = kv.waitChan[index]
	}
	kv.mu.Unlock()

	select{
	case op := <- waitChan:
		reply.Value = op.Value
		currTerm, leader := kv.rf.GetState()
		if leader && currTerm == term {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(kv.timeout):
		reply.Err = ErrWrongLeader
	}

	//elem, _ := kv.kvMap[args.Key]
	//reply.Value = elem
	//kv.commandMap[opnum] = elem

	kv.mu.Lock()
	delete(kv.waitChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	opnum := args.Opnum
	op := Op{Key: args.Key, Operation: "Put", Id: opnum, Value: args.Value}
	//log.Println("Put: ", op.Key, op.Id)
	// check if it is a duplicate rpc
	_, ok := kv.commandMap[opnum]
	if ok {
		reply.Err = OK
		//reply.Value = value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//log.Println("Put: not in commandMap")
	// if this operation hasn't executed
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		//log.Println("Put: not leader")
		return
	}
	//log.Println("Put: is leader")
	kv.mu.Lock()
	waitChan, ok := kv.waitChan[index]
	if !ok { // check if waitChan has message
		kv.waitChan[index] = make(chan *Op, 1)
		waitChan = kv.waitChan[index]
		//log.Println("Put: waitChan")
	}
	kv.mu.Unlock()

	select{
	case <- waitChan:
		//reply.Value = op.Value
		currTerm, leader := kv.rf.GetState()
		if leader && currTerm == term {
			reply.Err = OK
			//log.Println("Put: reply ok")
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(kv.timeout):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//log.Println("Append")
	kv.mu.Lock()
	opnum := args.Opnum
	op := Op{Key: args.Key, Operation: "Append", Id: opnum, Value: args.Value}

	// check if it is a duplicate rpc
	_, ok := kv.commandMap[opnum]
	if ok {
		reply.Err = OK
		//reply.Value = value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// if this operation hasn't executed
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	waitChan, ok := kv.waitChan[index]
	if !ok { // check if waitChan has message
		kv.waitChan[index] = make(chan *Op, 1)
		waitChan = kv.waitChan[index]
	}
	kv.mu.Unlock()

	select{
	case <- waitChan:
		//reply.Value = op.Value
		currTerm, leader := kv.rf.GetState()
		if leader && currTerm == term {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(kv.timeout):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) apply() {
	for kv.killed() == false {
		applyMessage := <- kv.applyCh
		kv.mu.Lock()
		if applyMessage.CommandValid {
			if op, ok := applyMessage.Command.(Op); ok {
				//log.Println("exec")
				// check duplicate based on op.Id
				if _, contains := kv.commandMap[op.Id]; !contains {
					if (op.Operation == "Get") {
						op.Value = kv.kvMap[op.Key]
					} else if (op.Operation == "Put") {
						kv.kvMap[op.Key] = op.Value
						kv.commandMap[op.Id] = op.Value // single execution
					} else if (op.Operation == "Append"){
						kv.kvMap[op.Key] += op.Value
						kv.commandMap[op.Id] = op.Value // single execution
					}
				}
				
				term, isLeader := kv.rf.GetState()
				if isLeader && applyMessage.CommandTerm == term {
					if waitCh, ok := kv.waitChan[applyMessage.CommandIndex]; ok {
						waitCh <- &op
					}
				}
			}
		}
		kv.mu.Unlock()
	}
}
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.commandMap = make(map[int64]string)
	kv.kvMap = make(map[string]string)
	kv.waitChan = make(map[int]chan *Op)
	kv.timeout = 600 * time.Millisecond

	go kv.apply();

	return kv
}
