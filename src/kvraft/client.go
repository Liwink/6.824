package raftkv

import "labrpc"
import "crypto/rand"
import mrand "math/rand"
import (
	"math/big"
	"fmt"
)

type Clerk struct {
	preLeaderId int
	servers     []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	ck.preLeaderId = 0
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf("[Clerk] ")
		fmt.Printf(format, a...)
		fmt.Print("\n ")
	}
	return
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{key, nrand()}

	ck.DPrintf("Get: %s", key)

	for true {
		reply := &GetReply{}
		ok := ck.servers[ck.preLeaderId].Call("KVServer.Get", args, reply)
		if !ok || reply.WrongLeader {
			ck.preLeaderId = mrand.Intn(len(ck.servers))
		} else if reply.Err == ErrNoKey {
			ck.DPrintf("Get ErrNoKey: %s", key)
			return ""
		} else if reply.Err == "" {
			ck.DPrintf("Get Done: Key: %s; Value: %s", key, reply.Value)
			return reply.Value
		}
	}

	// todo: timeout?
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, nrand()}

	ck.DPrintf("PutAppend: Key: %s; Value: %s, Op: %s", key, value, op)

	for true {
		reply := PutAppendReply{}
		ok := ck.servers[ck.preLeaderId].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.WrongLeader {
			ck.preLeaderId = mrand.Intn(len(ck.servers))
		} else if reply.Err == "" {
			ck.DPrintf("PutAppend Done: Key: %s; Value: %s, Op: %s", key, value, op)
			return
		} else {
			ck.DPrintf("PutAppend What's Wrong: reply: %v", reply)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
