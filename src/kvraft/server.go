package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	meetIndex int
	cmdC      map[interface{}]chan interface{}
	result    map[string]string
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.WrongLeader = true
		return
	}
	var ok bool
	reply.WrongLeader = false

	kv.cmdC[args.UniqueId] = make(chan interface{}, 1)
	kv.rf.Start(args.UniqueId)

	select {
	case <-kv.cmdC[args.UniqueId]:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value, ok = kv.result[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = ""
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	kv.cmdC[args.UniqueId] = make(chan interface{}, 1)
	kv.rf.Start(args.UniqueId)

	// fixme: timeout?
	timeoutC := make(chan interface{})
	//go func() {
	//	time.Sleep(300 * time.Millisecond)
	//	timeoutC <- struct {}{}
	//}()

	select {
	case <-kv.cmdC[args.UniqueId]:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = ""
		if args.Op == "Put" {
			kv.result[args.Key] = args.Value
		} else if args.Op == "Append" {
			kv.result[args.Key] += args.Value
		}
	case <-timeoutC:
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) listenApply() {
	var ok bool
	msg := <-kv.applyCh
	kv.mu.Lock()
	if msg.CommandValid && msg.CommandIndex > kv.meetIndex {
		_, ok = kv.cmdC[msg.Command]
		if ok {
			kv.cmdC[msg.Command] <- struct{}{}
		}
		kv.meetIndex = msg.CommandIndex
	}
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.meetIndex = -1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.listenApply()

	return kv
}
