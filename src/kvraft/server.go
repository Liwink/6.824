package raftkv

import (
	"labgob"
	"labrpc"
	//"log"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"raft"
	"strings"
	"sync"
	"time"
)

const Debug = 0

type Cmd struct {
	Key   string
	Value string
	Op    string
	// fixme: int64?
	UniqueId string
}

func dump(cmd Cmd) string {
	return fmt.Sprintf("%s;%s;%s;%s", cmd.Key, cmd.Value, cmd.Op, cmd.UniqueId)
}

func load(str string) Cmd {
	s := strings.Split(str, ";")
	return Cmd{
		s[0],
		s[1],
		s[2],
		s[3],
	}
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
	meetIndex    int
	cmdC         map[string]chan interface{}
	result       map[string]string
	committedCmd map[string]bool
}

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf("[Server %d] ", kv.me)
		fmt.Printf(format, a...)
		fmt.Print("\n ")
	}
	return
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
	kv.DPrintf("Get; args: %v", args)
	var ok bool
	reply.WrongLeader = false

	cmd := Cmd{
		Key:      args.Key,
		Op:       "Get",
		UniqueId: fmt.Sprintf("%v", args.UniqueId),
	}

	kv.mu.Lock()
	ch := make(chan interface{}, 1)
	kv.cmdC[cmd.UniqueId] = ch
	kv.mu.Unlock()

	kv.rf.Start(dump(cmd))

	select {
	case <-ch:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value, ok = kv.result[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = ""
		}
	}
	kv.DPrintf("Get Done; args: %v; reply: %v", args, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		return
	}

	kv.DPrintf("PutAppend; args: %v", args)
	reply.WrongLeader = false

	cmd := Cmd{
		args.Key,
		args.Value,
		args.Op,
		fmt.Sprintf("%v", args.UniqueId),
	}

	kv.mu.Lock()

	if _, ok := kv.committedCmd[cmd.UniqueId]; ok {
		kv.mu.Unlock()
		return
	}

	ch := make(chan interface{}, 1)
	kv.cmdC[cmd.UniqueId] = ch

	kv.mu.Unlock()

	kv.rf.Start(dump(cmd))

	select {
	case <-ch:
		reply.Err = ""
	// fixme: timeout?
	case <-time.After(time.Hour):
		reply.Err = ErrTimeout
	}
	kv.DPrintf("PutAppend Done; args: %v, reply: %v", args, reply)
}

func (kv *KVServer) listenApply() {
	for true {
		msg := <-kv.applyCh
		kv.DPrintf("Listen & Get msg %v", msg)
		kv.mu.Lock()
		// fixme: order by commandIndex
		// slidingWindow?

		cmd := load(msg.Command.(string))
		//_, ok := kv.committedCmd[cmd.UniqueId]
		if msg.CommandValid {
			//if msg.CommandValid && msg.CommandIndex > kv.meetIndex && !ok {

			// apply to kv
			_, ok := kv.committedCmd[cmd.UniqueId]
			if !ok {
				if cmd.Op == "Put" {
					kv.result[cmd.Key] = cmd.Value
				} else if cmd.Op == "Append" {
					kv.result[cmd.Key] += cmd.Value
				}

				kv.committedCmd[cmd.UniqueId] = true
			}

			_, ok = kv.cmdC[cmd.UniqueId]
			if ok {
				kv.cmdC[cmd.UniqueId] <- struct{}{}
				delete(kv.cmdC, cmd.UniqueId)
			}
			kv.meetIndex = msg.CommandIndex
		}
		kv.mu.Unlock()
	}
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
	kv.result = make(map[string]string)
	kv.cmdC = make(map[string]chan interface{})
	kv.committedCmd = make(map[string]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go func() {
		kv.DPrintf("Run pprof: %v", http.ListenAndServe("localhost:6060", nil))
	}()

	go kv.listenApply()

	return kv
}
