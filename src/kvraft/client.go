package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	opnum := nrand()
	args := GetArgs{key, opnum}
	reply := GetReply{}
	
	for {
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		
		if ok {
			if reply.Err == OK {
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				return ""
			}
			
		}
		// re-try by sending to different server
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	opnum := nrand()
	args := PutAppendArgs{key, value, opnum}
	reply := PutAppendReply{}
	
	for {
		ok := ck.servers[ck.leader].Call("KVServer."+op, &args, &reply)
		if ok {
			if reply.Err == OK {
				return
			}
			if reply.Err == ErrNoKey {
				return
			}
		}
		// re-try by sending to different server
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
