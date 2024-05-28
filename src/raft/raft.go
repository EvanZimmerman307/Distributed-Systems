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

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain

	state         string    // server state (follower, candidate, or leader)
	heartbeatTime time.Time // at which time will your heartbeat timeout
	electionTime  time.Time // when your election times out and another election will happen

	// Persistent state on all server
	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on all leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.log); err != nil {
		log.Fatalf("persist encode log error: %v", err)
	}
	if err := e.Encode(rf.currentTerm); err != nil {
		log.Fatalf("persist encode currentTerm error: %v", err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Fatalf("persist encode votedFor error: %v", err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	if err := d.Decode(&log); err != nil {
		fmt.Printf("readPersist decode log error: %v", err)
	}
	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("readPersist decode currentTerm error: %v", err)
	}
	if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("readPersist decode votedFor error: %v", err)
	}
	rf.log = log
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term of the conflicting entry in follower
	XIndex  int // index of the first entry of the conflicting term in follower
	XLen    int // log length
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//reply false if term < currentTerm, don't reset election timeout
	if args.Term < rf.currentTerm {
		fmt.Println("Hey! ", args.LeaderID, "you are not the leader")
		fmt.Println("I ", rf.me, "think the term is", rf.currentTerm, "but args.Term is", args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//case 3: there is not any conflict but the follower doesn't have the log
	if args.PrevLogIndex >= len(rf.log) { // the leader's PrevLogIndex is beyond the length of the follower's log
		fmt.Println("Server: ", rf.me, "log does not contain PrevLogIndex: ", args.PrevLogIndex)
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term // correct?
		//rf.persist()
		rf.state = "follower"
		ms := 600 + (rand.Int63() % 500)                                       // was 600
		rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond) //reset election timeout

		reply.XTerm = -1
		reply.XLen = len(rf.log) // + 1

		return
	}

	// Always check this even for heartbeats and not heartbeats
	//reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm, reset timeout?
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		fmt.Println("Conflicting log entry in follower: ", rf.log[args.PrevLogIndex])
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term // correct?
		//rf.persist()
		rf.state = "follower"                                                  // correct?
		ms := 600 + (rand.Int63() % 500)                                       // was 600
		rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond) //reset election timeout

		reply.XTerm = rf.log[args.PrevLogIndex].Term
		conflictingIndex := args.PrevLogIndex
		for rf.log[conflictingIndex].Term == rf.log[args.PrevLogIndex].Term {
			conflictingIndex-- // this will break when the terms no longer match
		}

		reply.XIndex = conflictingIndex + 1 // conflictingIndex will be the first index with a different term from the conflicting Term

		// should I delete everything in the follower's log after the conflict?

		return
	}

	//if an existing entry conflicts with a new one(same index, different terms), delete an existing entry and all that follow it
	if len(args.Entries) > 0 { // if nothing is in args.Entries it is just a hearbeat
		nextInd := args.PrevLogIndex + 1

		if len(rf.log) <= nextInd { //if there are no existing entries to compare
			print(" adding a log entry ")
			newEntries := make([]LogEntry, len(args.Entries))
			copy(newEntries, args.Entries)
			rf.log = append(rf.log, newEntries...)
			//rf.persist()
			print(rf.log)
		} else { //there are existing entries to compare, rf.log > next Ind
			for i := nextInd; i < nextInd+len(args.Entries); i++ { //just compare appendEntires rpc to matching values, outdated case

				if i == len(rf.log) { //nothing more to check, add anything remaining
					//append new entries not in the log
					newEntries := make([]LogEntry, len(args.Entries)-(i-nextInd)) // should be remaining entries(conflict and onwards)
					//copy
					copy(newEntries, args.Entries[i-nextInd:]) //from conflict onwards
					rf.log = append(rf.log, newEntries...)
					//rf.persist()
					break
				}

				if rf.log[i].Term != args.Entries[i-nextInd].Term { //conflict with args.Entries and rf.log
					// create a new log that is the length of the good entries
					newLog := make([]LogEntry, i)
					//copy the newLog
					copy(newLog, rf.log[:i])
					rf.log = newLog

					//append new entries not in the log
					newEntries := make([]LogEntry, len(args.Entries)-(i-nextInd)) // should be remaining entries(conflict and onwards)
					//copy
					copy(newEntries, args.Entries[i-nextInd:]) //from conflict onwards
					rf.log = append(rf.log, newEntries...)
					//rf.persist()
					break
				}
			}

		}
	}

	if args.LeaderCommit > rf.commitIndex { //can't commit something that's not in your log
		fmt.Println("LeaderCommit is greater than rf.commitIndex. LeaderCommit: ", args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1) //commitIndex = min(leaderCommit, index of last new entry)
		fmt.Println("New commitIndex: ", rf.commitIndex)
	}

	//only reset the election timeout for a successful response
	//generic response
	fmt.Println("Server: ", rf.me, "appended entries from leader: ", args.LeaderID)
	reply.Success = true
	rf.currentTerm = args.Term
	reply.Term = args.Term
	rf.state = "follower"
	ms := 600 + (rand.Int63() % 500)                                       // was 600
	rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond) //reset election timeout
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	fmt.Println("Received Request Vote from server: ", args.CandidateId)

	//lock raft to safely update fields
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//check if term > currentTerm
	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.votedFor = -1
		rf.currentTerm = args.Term
		//rf.persist()
	}

	// How to get lastLogTerm?
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId { //args.CandidateId < 0 { //updated log check
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			//rf.persist()
		}
	}

	//...Other things you need to consider, like setting election timeout

	ms := 600 + (rand.Int63() % 500)
	rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	// if this server isn't the leader, returns false.
	if rf.state != "leader" || rf.killed() { //rf.killed() make sense here???
		isLeader = false
		return index, term, isLeader
	}

	rf.mu.Lock()
	//and other operations?
	//create log entry and append
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	rf.persist()
	rf.mu.Unlock()
	//... other operations
	term = rf.currentTerm   // second return value is current term
	index = len(rf.log) - 1 // the first return value is the index that the command will appear at if it's ever committed.

	fmt.Println("Starting on new log entry: ", rf.log[index].Command, "for term: ", rf.currentTerm)
	// send the log to other nodes
	rf.BroadcastLog()
	fmt.Println("starting to broadcast log ")
	//DPrintf("starting to broadcast log")

	return index, term, isLeader
}

func (rf *Raft) BroadcastLog() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// if last log index >= nextIndex of a follower... send the follower the updates it needs
		if len(rf.log)-1 >= rf.nextIndex[peer] && !rf.killed() {
			go rf.sendLogEntries(peer)
		}
	}
}

func (rf *Raft) sendLogEntries(server int) {
	rf.mu.Lock()
	//defer rf.persist()
	if rf.state == "leader" {
		//rf.mu.Lock()
		//construct appendEntries args
		nextIndex := rf.nextIndex[server] //index of the next log entry to send to a server
		fmt.Println("Leader: ", rf.me, "is sending log entries to: ", server)
		fmt.Println("Follower's PrevLogIndex: ", nextIndex-1)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: nextIndex - 1,                   // index of log entry immediately preceding new ones
			PrevLogTerm:  rf.getEntry(nextIndex - 1).Term, //Term of prevlogIndex entry
			LeaderCommit: rf.commitIndex,
		}
		// Determine the length of the slice you want to copy
		length := len(rf.log[rf.nextIndex[server]:])

		// Create a new slice with the same length
		args.Entries = make([]LogEntry, length)

		// Copy the elements from the original slice to the new slice
		copy(args.Entries, rf.log[rf.nextIndex[server]:])

		rf.mu.Unlock()
		reply := &AppendEntriesReply{}

		//print(len(args.Entries))
		print(args.Entries)
		ok := rf.sendAppendEntries(server, args, reply) // send the log, if this rpc fails we should prob try again
		if ok {
			if reply.Success {
				rf.mu.Lock()
				newNext := args.PrevLogIndex + 1 + len(args.Entries)
				newMatch := args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = max(newNext, rf.nextIndex[server]) //update nextIndex
				rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])

				// if N > commitIndex & majority matchIndex[i] >= N, and log[n].Term > curretTerm
				// Basically, is the log successfully replicated?
				for N := rf.commitIndex + 1; N < len(rf.log); N++ {
					count := 1 // Count the number of servers that have this log entry
					for i, matchIdx := range rf.matchIndex {
						if i != rf.me && matchIdx >= N {
							count++ // matchIndex[i] >= N
						}
					}
					if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm { //if count is majority
						rf.commitIndex = N
						print(" majority updated log ")
						fmt.Println("new commit index ", rf.commitIndex)
						break //do we break here???
					}
				}
				rf.mu.Unlock()

			} else { // if not reply.Success
				//failure because of new leader or failure because of wrong logs
				rf.mu.Lock()
				if reply.Term > rf.currentTerm { //failure because you are not the right leader, update term and go back to follower
					rf.currentTerm = reply.Term
					rf.state = "follower"
					rf.persist() // not the leader so update term
					rf.mu.Unlock()
					return
				} else if reply.XTerm == -1 { //Case 3: there is not any conflict, but follower doesn’t have the log
					rf.nextIndex[server] = reply.XLen
					rf.mu.Unlock()
					go rf.sendLogEntries(server)
				} else if rf.log[reply.XIndex].Term != reply.XTerm { //Case 1: Leader does not have XTerm
					rf.nextIndex[server] = reply.XIndex
					rf.mu.Unlock()
					go rf.sendLogEntries(server)
				} else { //Case 2: Leader has XTerm
					startingIndex := reply.XIndex
					for rf.log[startingIndex].Term == rf.log[reply.XIndex].Term {
						startingIndex++
					}
					rf.nextIndex[server] = startingIndex
					rf.mu.Unlock()
					go rf.sendLogEntries(server)
				}
			}
		} else { // if RPC fails, try again, pause and try again?
			time.Sleep(time.Duration(100) * time.Millisecond)
			go rf.sendLogEntries(server)
		}
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) getEntry(ind int) LogEntry {
	return rf.log[ind]
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		// Your code here (2A)
		// leader repeat heartbeat during idle periods to prevent election timeouts (§5.2)
		if state == "leader" && time.Now().After(rf.heartbeatTime) {
			//rf.sendHeartBeat()
			rf.BroadcastHeartbeat()
		}

		// Check if a leader election should be started.
		// If election timeout elapses: start new election
		if time.Now().After(rf.electionTime) && state != "leader" && !rf.killed() {
			// begin election
			fmt.Println("Server: ", rf.me, " is about to raise an election")
			rf.raiseElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 120 //50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) BroadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// if last log index >= nextIndex of a follower... send the follower the updates it needs
		rf.mu.Lock()
		if rf.state == "leader" && !rf.killed() {
			rf.mu.Unlock()
			go rf.sendHeartBeat2(peer)
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat2(peer int) {
	rf.mu.Lock()
	//reset heartbeat timeout
	ms := 150
	rf.heartbeatTime = time.Now().Add(time.Duration(ms) * time.Millisecond)

	nextIndex := rf.nextIndex[peer]
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: nextIndex - 1,                   // index of log entry immediately preceding new ones
		PrevLogTerm:  rf.getEntry(nextIndex - 1).Term, //Term of prevlogIndex entry
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peer, args, reply) // send the log
	if ok {
		if !reply.Success {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm { //failure because you are not the right leader, update term and go back to follower
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.persist() //update term when bad heartbeat
				rf.mu.Unlock()
				return
			} else if reply.XTerm == -1 { //Case 3: there is not any conflict, but follower doesn’t have the log
				rf.nextIndex[peer] = reply.XLen
				rf.mu.Unlock()
				go rf.sendLogEntries(peer)
			} else if rf.log[reply.XIndex].Term != reply.XTerm { //Case 1: Leader does not have XTerm
				rf.nextIndex[peer] = reply.XIndex
				rf.mu.Unlock()
				go rf.sendLogEntries(peer)
			} else { //Case 2: Leader has XTerm
				startingIndex := reply.XIndex
				for rf.log[startingIndex].Term == rf.log[reply.XIndex].Term {
					startingIndex++
				}
				rf.nextIndex[peer] = startingIndex
				rf.mu.Unlock()
				go rf.sendLogEntries(peer)
			}
		}
	}
}

func (rf *Raft) raiseElection() {
	fmt.Println("Server: ", rf.me, "started an election")
	rf.mu.Lock()
	rf.state = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	voteCount := 1

	ms := 600 + (rand.Int63() % 500)
	rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		//Add Log there!!!
		go rf.candidateRequestVote(peer, &voteCount)
	}
}

func (rf *Raft) candidateRequestVote(peer int, voteCount *int) {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	reply := &RequestVoteReply{}
	rf.mu.Unlock()

	fmt.Println("Server: ", rf.me, " is about to request votes")
	Success := rf.sendRequestVote(peer, args, reply)

	if Success {

		// Lock the Raft to safely update its fields
		//rf.mu.Lock()
		//defer rf.mu.Unlock()

		//check 5.1
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			//rf.mu.Lock()
			rf.state = "follower"
			rf.currentTerm = reply.Term
			rf.persist()
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
		}

		if reply.VoteGranted {
			*voteCount++
			if *voteCount > len(rf.peers)/2 {
				rf.mu.Lock()
				//becomes Leader
				// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
				rf.state = "leader"

				//reinitialize nextIndex[] and matchIndex[]
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log) //= append(rf.nextIndex, len(rf.log)+1)
					rf.matchIndex[i] = 0          //= append(rf.matchIndex, 0)
				}

				fmt.Println("New Leader: ", rf.me, "the term is: ", rf.currentTerm)
				//var once sync.Once
				//once.Do(func() {
				//print("here")
				//go rf.sendHeartBeat()
				rf.mu.Unlock()
				rf.BroadcastHeartbeat() // send heartbeats after getting elected
				//})
			}
		}

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = "follower"
	ms := 600 + (rand.Int63() % 500)
	rf.electionTime = time.Now().Add(time.Duration(ms) * time.Millisecond)

	rf.currentTerm = 0
	rf.votedFor = -1

	//initialize the log for each raft server
	dummyEntry := LogEntry{
		Command: nil,
		Term:    0,
	}
	rf.log = append(rf.log, dummyEntry)

	//initialize nextIndex[] and matchIndex[] (2 and 1 because of dummyEntry)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	//heartbeat := 150
	//rf.heartbeatTime = time.Now().Add(time.Duration(heartbeat) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//periodically applied the newest comitted log entries to the channel
	go rf.applyLogs2Chann(applyCh)

	return rf
}

func (rf *Raft) applyLogs2Chann(applyCh chan ApplyMsg) {
	//for raft node is not killed {
	for !rf.killed() {
		//lock()
		rf.mu.Lock()

		appliedMsgs := []ApplyMsg{}           //create an array of log entries to commit
		for rf.commitIndex > rf.lastApplied { //commit all the log entries up to commitIndex
			fmt.Println("We have something to apply to Chann, ID: ", rf.me)
			rf.lastApplied++ //increment last applied with each entry
			fmt.Println("Index of last applied", rf.lastApplied)
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command, //the log whose index is rf.lastApplied
				CommandIndex: rf.lastApplied,
			})
			fmt.Println("appliedMsgs: ", appliedMsgs)
		}
		//Unlock
		rf.mu.Unlock()

		for _, msg := range appliedMsgs {
			applyCh <- msg
		}

		//Sleep for a period of time
		time.Sleep(10 * time.Millisecond)
	}
}
