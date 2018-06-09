package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// todo: 2a
	currentTerm int
	votedFor int
	majorityNum int

	isLeader bool
	isLeaderAlive bool

	commitIndex int
	lastApplied int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	// todo: 2a
	return rf.currentTerm, rf.isLeader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


type LogEntry struct {
	term int
	index int
	// command
	command interface{}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// todo: 2a
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// todo: 2a
	term int
	voteGranted bool
}

type AppendEntriesArgs struct {
	term int
	leaderId int
	prevLogIndex int
    prevLogTerm int
    entries []*LogEntry
    commitIndex int
}

type AppendEntriesReply struct {
	term int
	success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.term < rf.currentTerm {
		return
	}
    if args.term > rf.currentTerm {
        rf.currentTerm = args.term
    }

    // If candidate or leader, step down
    rf.isLeader = false

    // Reset election timeout
    rf.mu.Lock()
    rf.isLeaderAlive = true
    rf.mu.Unlock()

    // Return failure if log doesn’t contain an entry at
    // prevLogIndex whose term matches prevLogTerm

    // If existing entries conflict with new entries, delete all
    // existing entries starting with first conflicting entry

    // Append any new entries not already in the log

    // Advance state machine with newly committed entries
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		0,
		0,
		nil,
		0,
	}
	for i := 0; i < len(rf.peers); i++ {
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i, &args, &reply)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// todo: 2a
	if args.term > rf.currentTerm {
		rf.currentTerm = args.term
		rf.isLeader = false
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.candidateId) &&
			args.term >= rf.currentTerm && args.lastLogIndex >= rf.commitIndex {
		rf.votedFor = args.candidateId
		reply.voteGranted = true
	} else {
		reply.voteGranted = false
	}
	reply.term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVotes() {
	// vote for itself
	rf.votedFor = rf.me
	wg := sync.WaitGroup{}
	wg.Add(rf.majorityNum)

	// requests RPCs in parallel
	// todo: parallel
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		0,
		0,
	}
	for i := 0; i < len(rf.peers); i++ {
		go func() {
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)
			if reply.voteGranted {
				wg.Done()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// a. it wins the election
	// b. another server establish itself as a leader
	// c. a period of time goes by with no winner
	select {
	case <-valiedLeaderChan:
		rf.isLeaderAlive = true
	case <-done:
		rf.isLeader = true
	case <-electionTimeoutChan:
		fmt.Println("sendRequestVotes timeout")
	}
	return
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).


	return index, term, rf.isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// todo: 2a
	rf.votedFor = -1
	rf.isLeaderAlive = false
	rf.majorityNum = len(rf.peers) / 2 + 1

	// heartbeats
	go func() {
		for {
			time.Sleep(150 * time.Millisecond)
			if rf.isLeader {
				rf.sendHeartbeats()
			}
		}
	}()

	// selection
	go func() {
		for {
			time.Sleep(time.Duration(350 + rand.Intn(200)) * time.Millisecond)
			if rf.isLeader {
				continue
			}

			rf.mu.Lock()
			if rf.isLeaderAlive {
				rf.isLeaderAlive = false
			} else {
				rf.sendRequestVote()
			}
			rf.mu.Unlock()

			// election

		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
