package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs351/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type raftLogEntry struct {
	Term    int
	Command interface{}
	// NEED TO REDEFINE logEntry in piazza @273 Your code should not rely on any changes to config.go, as we will test it with a copy of the original config.go. In general the code in config.go is just for testing purposes. You can define your own entry struct in raft.go to use internally.
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // This peer's index into peers[]
	dead  int32               // Set by Kill()

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers:
	currentTerm   int           // from figure 2
	votedFor      int           // from figure 2
	role          int           // this represents role of the raft peer, (0 represents leader, 1 represents candidate, 2 represents follower)
	heartbeat     time.Time     // need this for the heartbeat function
	votesReceived int           // for leader election
	applyCh       chan ApplyMsg // to send committed entries

	// 3B stuff for log related state
	log []raftLogEntry // renamed to avoid conflict with config.go
	// type logEntry struct {
	// 	valid   bool
	// 	command interface{}
	// }
	// Volatile state on all servers:
	// NEED TO REDEFINE logEntry in piazza @273 Your code should not rely on any changes to config.go, as we will test it with a copy of the original config.go. In general the code in config.go is just for testing purposes. You can define your own entry struct in raft.go to use internally.
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // proteting access to vars
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.role == 0 // 0 is leader

	return term, isleader
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// from figure 2A
	// Arguments:
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// from figure 2A under results
	// Results:
	Term        int
	VoteGranted bool
}

// from figure 2a
type AppendEntriesArgs struct {
	// Arguments:
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []raftLogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Results:
	Term    int
	Success bool
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	// 	Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return // reject if candidate term is lower
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = 2 // step down
	}
	// if votedFor is null aka -1
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.heartbeat = time.Now() // reset timer
	}
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// copying the example from above for appendentries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendRequestVotes() {
	rf.mu.Lock() // lock these so other routines cannot access
	term := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.receiveRequestVoteAndReply(reply)
			}
		}(i)
	}
}

func (rf *Raft) receiveRequestVoteAndReply(reply *RequestVoteReply) {
	rf.mu.Lock() // lock everything, since no go routine defer til end of function
	defer rf.mu.Unlock()

	if rf.role != 1 { // if not a canddiate then it shouldnt get voted
		return
	}

	// by that logic If the replying peer has a higher term, step down to follower beacuse this node is behind in election term, can't be leader if behind
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1          // reset votedFor, no vote in the new term
		rf.role = 2               // become follower
		rf.heartbeat = time.Now() // reset election timer
		return
	}
	// if received a vote
	if reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 { // won the election
			rf.role = 0               // become leader
			rf.heartbeat = time.Now() // reset timer
			// Initialize nextIndex and matchIndex after election (VOLATILE SATE ON LEADERS STATE)
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log) // Last log index + 1
				rf.matchIndex[i] = 0
			}
			go rf.sendHeartbeats() // need to implement to send heartbeats
		}
	}
}

// fig 2a
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm { // 5.1 case reply false
		reply.Success = false
		return
	}
	// same logic as requestvote
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = 2 // Become follower
	}

	rf.heartbeat = time.Now() // reset election timer
	rf.role = 2               // make sure it's still a follower

	// 3B
	// need to implement
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	for i := 0; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term != args.Entries[i].Term {
				rf.log = rf.log[:index] // Truncate log at conflict
				break
			}
		} else {
			break
		}
	}

	// 4. Append any new entries not already in the log
	for i := 0; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i
		if index >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		// if commitIndex has been changed try to apply the committed entries
		go rf.applyCommittedEntries()
	}
}

func (rf *Raft) receiveAppendEntriesAndReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rules for Servers, All Servers
	// If commitIndex > lastApplied: icrement increment lastApplied, apply log[lastApplied] to state machine
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = 2 // step down to follower same logic as before its behind so cant be leader
		rf.heartbeat = time.Now()
		return
	}

	// under rules for servers, leaders
	// if successful : update nextIndex and match Index for follower
	// if not then decremtnt nextIndex and retry

	// if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commmitIndex = N
	if reply.Success {
		// PrevLogIndex: index of log entry immediately preceding new ones
		// Entries[]: log entries to store (empty for heartbeat; may send more than one for efficiency)
		// Entries:      log[prevLogIndex+1:],
		// PrevLogIndex: prevLogIndex,
		// the next index will be the prev index and then the current slice of entries starting from prevLogIndex + 1
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		// also need to update commit Index, and to do that we need to make sure more than half of the cluster is consistent
		// now we check if there is an N
		for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
			N := 1 // count leader itself
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n && rf.log[n].Term == rf.currentTerm {
					N++
				}
			}
			if N > len(rf.peers)/2 { // if N becomes majority aka more than half
				rf.commitIndex = n
				go rf.applyCommittedEntries() // this we'd parallelize the commit process and we'd implement later
				break
			}
		}
	} else {
		rf.nextIndex[server] = max(1, rf.nextIndex[server]-1) // decrement and retry
	}
}

func (rf *Raft) applyCommittedEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// RUles for Servers, All Servers
	// If commit index > last applied: increment last applied apply log[lastapplied] to state machihne
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != 0 { // if not a leader anymore
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		leaderId := rf.me

		// adding onto 3A for 3B
		// need to update the new things like log, nextIndex, commitIndex
		// For each non-leader:

		// Calculate prevLogIndex and prevLogTerm
		// Send all entries after nextIndex
		// Include leader's commitIndex
		commitIndex := rf.commitIndex
		log := append([]raftLogEntry{}, rf.log...) // Copy log
		nextIndex := append([]int{}, rf.nextIndex...)
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				prevLogIndex := nextIndex[server] - 1
				var prevLogTerm int
				if prevLogIndex > 0 {
					prevLogTerm = log[prevLogIndex].Term
				}

				args := &AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      log[prevLogIndex+1:], // this handles the if last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
					LeaderCommit: commitIndex,
				}
				reply := &AppendEntriesReply{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
				// under figure 2a append entries
				if rf.sendAppendEntries(server, args, reply) {
					rf.receiveAppendEntriesAndReply(server, args, reply) // 2B need to add server, args so we can update the nextIndex and matchIndex
				}
			}(i)
		}
		// The Tester requires that the leader send heartbeat RPCs no more than ten times per second.
		time.Sleep(100 * time.Millisecond)
		// Your code may have loops that repeatedly check for certain events. Don't have these loops execute continuously without pausing, since that will slow your implementation enough that it fails tests. Use Go's condition variables, or insert a time.Sleep(10 * time.Millisecond) in each loop iteration.
	}
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // had a race condition because it was accessing state concurrently

	term := rf.currentTerm
	isLeader := false // start as false
	index := len(rf.log)
	if rf.role == 0 { // 0 is leader
		isLeader = true
	}

	// Your code here (3B).

	// If this server isn't the leader, returns false.
	if !isLeader {
		return index, term, isLeader
	}
	// Otherwise start the agreement and return immediately.
	// create a new log entry
	entry := raftLogEntry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// from 2A
		// Candidates (§5.2):
		// • On conversion to candidate, start election:
		// • Increment currentTerm
		// • Vote for self
		// • Reset election timer
		// • Send RequestVote RPCs to all other servers
		rf.mu.Lock()
		if rf.role != 0 { // If not leader (0 = leader, 1 = candidate, 2 = follower)
			elapsed := time.Since(rf.heartbeat)
			timeout := time.Duration(200+rand.Intn(200)) * time.Millisecond // from the paper To
			// prevent split votes in the first place, election timeouts are
			// chosen randomly from a fixed interval (e.g., 150–300ms)

			// The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds. Because the Tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.
			if elapsed >= timeout {
				rf.role = 1               // become candidate
				rf.currentTerm++          // increment term
				rf.votedFor = rf.me       // vote for self
				rf.votesReceived = 1      // count self-vote
				rf.heartbeat = time.Now() // reset timer
				rf.mu.Unlock()
				rf.sendRequestVotes() // send RPC to all other servers, start the election
				continue
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = 2
	rf.heartbeat = time.Now()
	rf.votesReceived = 0
	rf.applyCh = applyCh

	// in State that wasn't included in 3A
	// nextIndex[] for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	//
	// matchIndex[] for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	//
	// log[] log entries; each entry contains command
	// for state machine, and term when entry
	// was received by leader (first index is 1)
	//
	// lastApplied index of highest log entry applied to state
	// machine (initialized to 0, increases
	// monotonically)

	rf.nextIndex = make([]int, len(peers)) // this is VOLATILE, we need to reinitialize it after candidate becomes leader receiveRequestVoteAndReply, start as len(peers) to prevent problems
	rf.matchIndex = make([]int, len(peers))
	rf.lastApplied = 0
	rf.log = make([]raftLogEntry, 1) // first index is 1

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
