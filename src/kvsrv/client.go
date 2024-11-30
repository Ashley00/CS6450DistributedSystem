package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
//import "fmt"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	opnum := nrand()
	args := GetArgs{key, opnum, false}
	reply := GetReply{}
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	// re-send if message lost
	for ok == false {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}

	// send free server memory request
	args_free := GetArgs{key, opnum, true}
	reply_free := GetReply{}
	ok_free := ck.server.Call("KVServer.Get", &args_free, &reply_free)
	for ok_free == false {
		ok_free = ck.server.Call("KVServer.Get", &args_free, &reply_free)
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	opnum := nrand()
	args := PutAppendArgs{key, value, opnum, false}
	reply := PutAppendReply{}
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	// re-send if message lost
	for ok == false {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}

	// send free server memory request
	args_free := PutAppendArgs{key, value, opnum, true}
	reply_free := PutAppendReply{}
	ok_free := ck.server.Call("KVServer."+op, &args_free, &reply_free)
	for ok_free == false {
		ok_free = ck.server.Call("KVServer."+op, &args_free, &reply_free)
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
