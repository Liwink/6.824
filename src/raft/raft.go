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
	"strconv"
	"fmt"
	"bytes"
	"labgob"
)

// import "bytes"
// import "labgob"

type PersistData struct {
	CurrentTerm int
	VotedFor 	int
	Logs		[]*LogEntry
}

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

const followState = "follower"
const leaderState  = "leader"
const candidateState  = "candidate"

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
	totalNum int

	killChan chan struct{}
	validLeaderChan chan int
	name int

	currentState string

	isLeaderAlive bool

	// 2B
	logs []*LogEntry
	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int
	applyCh chan ApplyMsg
	applyIndex int

	isSendingLog bool
	heartbeatIndex int

}

func (rf *Raft)log(str string)  {
	fmt.Println(rf.me, rf.currentTerm, rf.currentState, str, rf.name)
}

func (rf *Raft) logEntries()  {
	fmt.Println(rf.me, rf.currentTerm, rf.currentState, rf.name)
	for _, e := range rf.logs {
		fmt.Printf("%d-(%d)-%d; ", e.Index, e.Term, e.Command)
	}
	fmt.Print("\n")
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	// todo: 2a
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == leaderState
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.log(fmt.Sprintf("persist persister"))
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	logs := rf.logs
	//if rf.commitIndex > -1 {
		//logs = rf.logs[:rf.commitIndex + 1]
	//}
	vars := PersistData{
		VotedFor: rf.votedFor,
		CurrentTerm: rf.currentTerm,
		Logs: logs,
	}
	e.Encode(&vars)
	rf.log(fmt.Sprintf("persist: currentTerm: %d; votedFor: %d; len(rf.logs): %d", rf.currentTerm, rf.votedFor, len(rf.logs)))
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.log(fmt.Sprintf("readPersist persister"))
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var vars PersistData
	if d.Decode(&vars) != nil {
		panic("readPersist")
	} else {
		rf.currentTerm = vars.CurrentTerm
		rf.votedFor = vars.VotedFor
		rf.logs = vars.Logs
	}
	rf.log(fmt.Sprintf("readPersist: currentTerm: %d; votedFor: %d; len(rf.logs): %d", rf.currentTerm, rf.votedFor, len(rf.logs)))
	rf.commitIndex = len(rf.logs) - 1
}


type LogEntry struct {
	Term int
	Index int
	// command
	Command interface{}
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// todo: 2a
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// todo: 2a
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []*LogEntry
	CommitIndex int
	ApplyIndex int
	HeartbeatIndex int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	PrevLogTerm int
	PrevLogIndex int
}

func (rf *Raft) apply(applyIndex int)  {
	if applyIndex >= len(rf.logs) {
		return
	}

	for i := rf.applyIndex + 1; i <= applyIndex; i++ {
		rf.applyCh <- ApplyMsg{
			true,
			rf.logs[i].Command,
			// FIXME: +1 to meet the test index requirement
			i + 1,
		}
		rf.log(fmt.Sprintf("Apply: %d, %d", i, rf.logs[i].Command))
	}

	if applyIndex > rf.commitIndex {
		rf.commitIndex = applyIndex
	}
	if applyIndex > rf.applyIndex {
		rf.applyIndex = applyIndex
	}
	rf.persist()

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	} else {
		rf.updateTerm(args.Term)
		rf.votedFor = args.LeaderId
		reply.Term = rf.currentTerm
	}

	// todo: compare last log term
	// If existing entries conflict with new entries, delete all
	// existing entries starting with first conflicting entry

	// Append any new entries not already in the log

	// Advance state machine with newly committed entries


	rf.isLeaderAlive = true
	// heartbeat
	if len(args.Entries) == 0 {
		reply.Success = true
		go rf.apply(args.ApplyIndex)
		rf.log(fmt.Sprintf("follower: received heartbeats %d", args.HeartbeatIndex))
		return
	}

	if (args.PrevLogIndex == -1 ) || (args.PrevLogIndex < len(rf.logs) && rf.logs[args.PrevLogIndex].Term == args.Term) {
		// for concurrent commit, the order is not guaranteed.
		// commitIndex 3 may arrival behind commitIndex 5
		// so do not overwrite the entries beyond args.CommitIndex
		var index int
		for i, entry := range args.Entries {
			index = args.PrevLogIndex + i + 1
			if index < len(rf.logs) {
				rf.logs[index] = entry
			} else {
				rf.logs = append(rf.logs, entry)
			}
		}
		// truncate the isolated leader's potential invalid logs
		if index + 1 < len(rf.logs) && rf.logs[index + 1].Term < rf.logs[index].Term {
			rf.logs = rf.logs[:index + 1]
		}
		//rf.commitIndex = args.CommitIndex
		reply.Success = true
		rf.logEntries()
	} else {
		if len(rf.logs) == 0 {
			reply.PrevLogTerm = -1
		} else if args.PrevLogIndex < len(rf.logs) {
			reply.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		} else {
			reply.PrevLogTerm = rf.logs[len(rf.logs) - 1].Term
		}
		reply.PrevLogIndex = min(args.PrevLogIndex, len(rf.logs))
	}
	//rf.persist()
	rf.log(fmt.Sprintf("AppendEntries: len(logs) %d", len(rf.logs)))
	rf.logEntries()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	applyIndex := rf.applyIndex
	rf.heartbeatIndex += 1
	heartbeatIndex := rf.heartbeatIndex
	rf.mu.Unlock()

	timeoutChan := make(chan struct{}, 1)
	aliveChan := make(chan struct{}, rf.totalNum)
	done := make(chan struct{}, 1)
	for i := 0; i < rf.totalNum; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := AppendEntriesArgs{
				currentTerm,
				rf.me,
				0,
				0,
				nil,
				commitIndex,
				min(applyIndex, rf.matchIndex[i]),
				heartbeatIndex,
			}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			rf.log(fmt.Sprintf("received %d response from %d: %v, %v", heartbeatIndex, i, ok, reply.Success))
			if ok && reply.Success == true {
				aliveChan <- struct{}{}
			}
		}(i)
	}
	go func() {
		for i := 0; i < rf.majorityNum-1; i++ {
			<-aliveChan
		}
		done <- struct{}{}
	}()

	go func() {
		time.Sleep(250 * time.Millisecond)
		timeoutChan <- struct{}{}
	}()

	select {
	case <-done:
		rf.mu.Lock()
		rf.log(fmt.Sprintf("received majority responses %d", heartbeatIndex))
		rf.isLeaderAlive = true
		rf.mu.Unlock()
	case <-timeoutChan:
		rf.mu.Lock()
		rf.log(fmt.Sprintf("did not get majority responses, timeout %d", heartbeatIndex))
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isSendingLog {
		return
	}
	for index, i := range rf.matchIndex {
		if index == rf.me {
			continue
		}
		if i < len(rf.logs) - 1 {
			go rf.appendEntries(len(rf.logs) - 1)
			return
		}
	}
	//go rf.appendEntries(len(rf.logs) - 1)
	//go rf.appendEntries(rf.commitIndex)
}

func (rf *Raft) appendEntries(commitIndex int) {
	// TODO: stop it when changing leader...?
	rf.mu.Lock()
	rf.isSendingLog = true
	rf.mu.Unlock()

	defer func() {
		rf.mu.Lock()
		rf.isSendingLog = false
		rf.mu.Unlock()
	}()

	rf.log(fmt.Sprintf("appendEntries: %d", commitIndex))
	rf.log(fmt.Sprintf("nextIndex: %v", rf.nextIndex))
	rf.log(fmt.Sprintf("matchIndex: %v", rf.matchIndex))

	receivedCh := make(chan struct{}, rf.totalNum)
	done := make(chan struct{}, 1)
	timeoutCh := make(chan struct{}, 1)
	finishCh := make(chan struct{}, 1)

	for i := 0; i < rf.totalNum; i++ {
		//
		if i == rf.me {
			continue
		}
		go func(i int) {
			// TODO: to ensure only one goroutine is working for one specific follower
			for {
				rf.mu.Lock()
				if rf.nextIndex[i] > commitIndex {
					receivedCh <- struct{}{}
					rf.mu.Unlock()
					return
				}
				prevIndex := rf.nextIndex[i] - 1
				term := 0
				// FIXME
				if prevIndex > -1 && prevIndex < len(rf.logs) {
					term = rf.logs[prevIndex].Term
				}
				logs := rf.logs[prevIndex + 1:commitIndex + 1]
				rf.log(fmt.Sprintf("Send logs: prevIndex %d, commitIndex %d, len(logs) %d", prevIndex, commitIndex, len(logs)))
				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevIndex,
					term,
					logs,
					commitIndex,
					rf.applyIndex,
					rf.heartbeatIndex,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					return
				}
				if reply.Success {
					// TODO: success
					rf.log(fmt.Sprintf("Send log %d to %d succeed", commitIndex, i))
					rf.mu.Lock()
					// nextIndex fail -> decrease; success -> increase
					rf.nextIndex[i] = max(args.CommitIndex + 1, rf.nextIndex[i])
					rf.matchIndex[i] = max(args.CommitIndex, rf.matchIndex[i])
					rf.mu.Unlock()
					receivedCh <- struct{}{}
					return
				} else if reply.Term > rf.currentTerm {
					// TODO
					rf.log("CurrentTerm is behind")
					return
				} else {
					rf.mu.Lock()
					if rf.currentState != leaderState {
						rf.log("Stop retrying sending log")
						rf.mu.Unlock()
						return
					}
					rf.log("Retry sending log")
					//rf.nextIndex[i] = args.PrevLogIndex - 1
					//rf.nextIndex[i] = reply.PrevLogIndex - 1
					rf.nextIndex[i] = 0
					for j := reply.PrevLogIndex - 1; j > 0; j-- {
						if rf.logs[j].Term <= reply.PrevLogTerm {
							rf.nextIndex[i] = j
							break
						}
					}
					if rf.nextIndex[i] < 0 {
						rf.nextIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}

	go func() {
		for i := 0; i < rf.totalNum - 1; i++ {
			<-receivedCh
			if i == rf.majorityNum - 2 {
				done <- struct{}{}
			}
		}
		finishCh <- struct {}{}
	}()

	go func() {
		time.Sleep(150 * time.Millisecond)
		timeoutCh <- struct{}{}
	}()

	for {
		select {
		case <-done:
			rf.mu.Lock()
			rf.log(fmt.Sprintf("commitIndex done %d", rf.commitIndex))
			rf.isLeaderAlive = true
			if rf.commitIndex < commitIndex {
				rf.commitIndex = commitIndex
			}
			if rf.applyIndex < commitIndex && rf.logs[rf.commitIndex].Term >= rf.currentTerm {
				go rf.apply(commitIndex)
			}
			rf.mu.Unlock()
		case <-timeoutCh:
			rf.mu.Lock()
			rf.log(fmt.Sprintf("leader: wait for appendEntities %d, timeout", commitIndex))
			rf.mu.Unlock()
			return
		case <- finishCh:
			rf.mu.Lock()
			rf.log("leader: all nodes finished appendEntities")
			rf.mu.Unlock()
			return
		}
	}

}

func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		rf.log("update term and become a follower")
		// lock write term
		rf.log("term update before")
		rf.currentTerm = term
		rf.log("term update after")
		// Reset election timeout
		// fixme: what if currentState change to LeaderState
		if rf.currentState == candidateState {
			rf.validLeaderChan <- 0
		}
		rf.currentState = followState
		rf.votedFor = -1
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// todo: 2a
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm

	lastLogTerm := 0
	lastLogIndex := -1
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs) - 1].Term
		lastLogIndex = rf.logs[len(rf.logs) - 1].Index
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		((args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rf.votedFor = args.CandidateId
		rf.log("follow: vote for " + strconv.Itoa(args.CandidateId))
		reply.VoteGranted = true
		rf.isLeaderAlive = true
	} else {
		rf.log("refuse to vote for" + strconv.Itoa(args.CandidateId))
		rf.log(fmt.Sprintf("args.LastLogIndex %d; rf.commitIndex %d", args.LastLogIndex, rf.commitIndex))
		reply.VoteGranted = false
	}

	rf.persist()
	rf.mu.Unlock()
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
	rf.mu.Lock()
	rf.log("term increase before")
	rf.currentTerm += 1
	rf.log("term increase after")
	rf.votedFor = rf.me

	lastLogIndex := -1
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		// fixme: since the logs index started from -1 now
		lastLogIndex = rf.logs[len(rf.logs) - 1].Index
		lastLogTerm = rf.logs[len(rf.logs) - 1].Term
	}

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lastLogIndex,
		lastLogTerm,
	}
	rf.mu.Unlock()


	responseChan := make(chan bool, rf.totalNum)
	timeout := make(chan struct{}, 1)

	// requests RPCs in parallel
	for i := 0; i < rf.totalNum; i++ {
		go func(i int) {
			if i == rf.me {
				return
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)

			rf.mu.Lock()
			// todo: if its term is out of date, it immediately reverts to follower state
			if reply.Term > rf.currentTerm {
				rf.updateTerm(reply.Term)
			}
			rf.mu.Unlock()

			if reply.VoteGranted {
				responseChan <- true
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < rf.majorityNum - 1; i++ {
			<- responseChan
		}
		close(done)
	}()

	// a. it wins the election
	// b. another server establish itself as a leader
	// c. a period of time goes by with no winner
	go func() {
		time.Sleep(300 * time.Millisecond)
		timeout <- struct{}{}
	}()

	// fixme: lock?
	select {
	case <-rf.validLeaderChan:
		rf.mu.Lock()
		rf.currentState = followState
		rf.isLeaderAlive = true
		rf.log("candidate: another leader established or should update term")
		rf.mu.Unlock()
	case <-done:
		rf.mu.Lock()
		rf.currentState = leaderState
		rf.isLeaderAlive = true
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.commitIndex + 1
		}
		rf.log("candidate: selected as a leader")
		rf.logEntries()
		rf.mu.Unlock()

		go rf.sendHeartbeats()
		//case <-rf.electionTimeoutChan:
	case <-timeout:
		rf.mu.Lock()
		rf.currentState = followState
		rf.log("candidate: sendRequestVotes timeout")
		rf.mu.Unlock()
	}
	return
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and **return immediately**. **there is no guarantee that this
// command will ever be committed to the Raft log**, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState == leaderState {
		rf.log(fmt.Sprintf("start %v", command))
	}

	if rf.currentState != leaderState {
		return len(rf.logs), rf.currentTerm, false
	}

	// Your code here (2B).
	// fixme: index start from -1 -> 0?
	rf.logs = append(rf.logs, &LogEntry{rf.currentTerm, len(rf.logs) - 1, command})
	rf.logEntries()
	//rf.commitIndex += 1

	if !rf.isSendingLog {
		go rf.appendEntries(len(rf.logs) - 1)
	}

	// TODO: update state

	rf.persist()

	// FIXME: +1 to meet the test index requirement
	return len(rf.logs), rf.currentTerm, rf.currentState == leaderState
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.log("Kill")
	rf.killChan <- struct{}{}
	rf.killChan <- struct{}{}
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) electionDaemon() {
	for {
		time.Sleep(time.Duration(550 + rand.Intn(300)) * time.Millisecond)

		// leader should be timeout and start a election as well
		// to stop receiving client `start` requests

		select {
		case <- rf.killChan:
			return
		default:
		}

		rf.mu.Lock()
		if rf.isLeaderAlive {
			rf.isLeaderAlive = false
		} else {
			rf.log("candidate: start election")
			rf.currentState = candidateState
			rf.persist()
			go rf.sendRequestVotes()
		}
		rf.mu.Unlock()

		// election
	}
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

	rf.name = rand.Intn(10000)

	rf.mu.Lock()
	rf.log("make")
	rf.mu.Unlock()

	// Your initialization code here (2A, 2B, 2C).
	// todo: 2a
	rf.votedFor = -1
	rf.isLeaderAlive = false
	rf.totalNum = len(rf.peers)
	rf.majorityNum = rf.totalNum / 2 + 1
	rf.currentState = followState
	rf.validLeaderChan = make(chan int, 1)

	rf.killChan = make(chan struct{}, 2)

	// 2b
	rf.commitIndex = -1
	rf.logs = make([]*LogEntry, 0)

	rf.isSendingLog = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, rf.totalNum)
	rf.matchIndex = make([]int, rf.totalNum)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = -1
	}
	rf.applyCh = applyCh
	rf.applyIndex = -1

	rf.heartbeatIndex = 0

	// heartbeats
	go func() {
		for {
			time.Sleep(150 * time.Millisecond)
			select {
			case <- rf.killChan:
				return
			default:

			}
			rf.mu.Lock()
			if rf.currentState == leaderState {
				go rf.sendHeartbeats()
			}
			rf.mu.Unlock()
		}
	}()

	// selection
	go rf.electionDaemon()



	return rf
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}
