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
	"lab5/labgob"

	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"lab5/constants"
	"lab5/labrpc"
	"lab5/logger"
)

const (
	FOLLOWER byte = iota
	CANDIDATE
	LEADER
)

const ELECTION_TIMEOUT = 150 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	appendMutexes []sync.Mutex

	logger *logger.Logger
	// Your data here (4A, 4B, 4C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role          byte // can be one of: FOLLOWER, CANDIDATE, LEADER
	electionTimer *time.Timer

	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// leader only volatile state
	nextIndex  []int // -> the next entry to send
	matchIndex []int // -> highest confirmed replicated index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (4A).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	role := rf.role
	rf.mu.Unlock()
	return currentTerm, role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (4C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// encode persistent state into bytes (currentTerm, votedFor, log)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}

	err = e.Encode(rf.votedFor)
	if err != nil {
		panic(err)
	}

	err = e.Encode(rf.log)
	if err != nil {
		panic(err)
	}

	// save raftstate in persister
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("persistence error -- persistent state is nil")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int // candidate term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of the candidate's last log term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  // to confirm an incoming reply is for the correct term
	VoteGranted bool // true if we vote for them
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// request is not in right term, reject
	if args.Term < rf.currentTerm {
		rf.logger.Log(0, "not voting for %d because term is too low (their term = %d), our term = % d", args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	rf.checkTermAndTransitionToFollower(true, args.Term)

	// already voted in this term, reject
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// get the term and index of our most up to date log entry
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	rf.logger.Log(0, "my last log index is %d and incomming is %d", lastLogIndex, args.LastLogIndex)

	// we are more up to date than the candidate, reject
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}

	// otherwise, we vote for this candidate!
	rf.votedFor = args.CandidateId
	rf.persist()

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	rf.logger.Log(0, "vote granted for %d", args.CandidateId)
	rf.resetElectionTimer(true)
	return
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	IndexNeeded int
	TermNeeded  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.IndexNeeded = 0
	reply.TermNeeded = -1

	rf.logger.Log(0, "got append entries from %d, with %d new entries, leader commit index %d, prev log index", args.LeaderId, len(args.Entries), args.LeaderCommit)

	// 1. reply false if term < current term
	if args.Term < rf.currentTerm {
		rf.logger.Log(0, "we are not in the same term as the leader who sent append entries. leader = %d, us = %d", args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	// 2. make sure we are a follower
	rf.transitionToFollower(true, args.Term)
	//rf.checkTermAndTransitionToFollower(true, args.Term)
	//rf.resetElectionTimer(true)

	// we're not up to date enough to process these entries (leader will fail backwards)
	if args.PrevLogIndex >= len(rf.log) {
		rf.logger.Log(0, "we're not up to date enough to process these entries (prev log index = %d), ours %d", args.PrevLogIndex, len(rf.log)-1)
		reply.IndexNeeded = len(rf.log) // tell leader where to backtrack to
		return
	}

	// we have something in the prev log index, but the terms do not match
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex

		// find the first entry of conflicting term, optimize backtrack so we don't use extra RPCs
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != conflictTerm {
				conflictIndex = i + 1
				break
			}
			if i == 0 {
				conflictIndex = 0
			}
		}
		rf.logger.Log(0, "(append entries) conflict: conflictTerm %d, conflictIndex %d", conflictTerm, conflictIndex)

		// send both back
		reply.IndexNeeded = conflictIndex
		reply.TermNeeded = rf.log[conflictIndex].Term
		return
	}

	rf.logger.Log(0, "Log before truncation %v", rf.log)
	startIndex := args.PrevLogIndex + 1

	for i, entry := range args.Entries {
		index := startIndex + i
		// if an entry already exists at this index, check for any conflict.
		if index < len(rf.log) {
			if rf.log[index].Term != entry.Term || rf.log[index].Command != entry.Command {
				rf.log = rf.log[:index]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()

	// update commit index as before
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}
	reply.Term = rf.currentTerm
	reply.IndexNeeded = len(rf.log)
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (4B).
	rf.mu.Lock()
	index := len(rf.log)

	isLeader := rf.role == LEADER
	term := rf.currentTerm

	// not leader, immediately return false
	if !isLeader {
		rf.mu.Unlock()
		return 0, term, false
	}

	entry := LogEntry{Term: term, Index: index, Command: command}
	rf.logger.Log(0, "STARTING AGREEMENT FOR %d, in term %d", command, rf.currentTerm)
	rf.logger.Log(0, "Current log: %v", rf.log)

	// pop the new entry in our log
	rf.log = append(rf.log, entry)
	rf.persist()

	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()

	// update all our followers
	go rf.sendAppendEntriesToFollowers()

	return index + 1, term, isLeader
}

// routine that constantly checks for new commited entries and sends them over the apply ch
func (rf *Raft) checkAndApply(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.lastApplied < rf.commitIndex {
			rf.logger.Log(0, "new entries, sending throguh apply ch")
			first := rf.lastApplied + 1
			toApply := rf.log[first : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex

			for i, entry := range toApply {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: first + i + 1,
				}
				rf.logger.Log(0, "sending cmd %v", entry.Command)
				applyCh <- applyMsg
				//		rf.logger.Log(0, "committed index = %d, log len = %d", rf.commitIndex, len(rf.log))
			}
			rf.mu.Unlock()

		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
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

// updates the commit index of the leader to what a majority of nodes have confirmed they have
func (rf *Raft) updateCommitIndex() {
	//matches := make([]int, len(rf.peers))
	//copy(matches, rf.matchIndex)
	//matches[rf.me] = len(rf.log) - 1
	//sort.Ints(matches)
	//
	//// median of the matches means that at least N/2 nodes have the msg at the index
	//medianReplicatedIndex := matches[(len(matches))/2]
	//
	//if medianReplicatedIndex > rf.commitIndex &&
	//	medianReplicatedIndex < len(rf.log) &&
	//	rf.log[medianReplicatedIndex].Term == rf.currentTerm {
	//
	//	oldCommitIndex := rf.commitIndex
	//	rf.commitIndex = medianReplicatedIndex
	//	rf.logger.Log(0, "Updated commit index: %d → %d", oldCommitIndex, medianReplicatedIndex)
	//}

	lastLogIndex := len(rf.log) - 1

	for i := rf.commitIndex + 1; i <= lastLogIndex; i++ {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}

		count := 0
		for j := range rf.peers {
			if rf.matchIndex[j] >= i {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			break
		}

	}
}

// used for heartbeat and append entries
func (rf *Raft) sendAppendEntriesToFollowers() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return
	}

	currentTerm := rf.currentTerm
	me := rf.me

	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}

		go func(i int) {
			// only allow for one append entry to be in flight at a time

			// loop until we have success, (we continue if follower is out of date)
			for {
				rf.mu.Lock()
				if rf.role != LEADER || rf.currentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}

				// next index that we think the client needs
				nextIndex := rf.nextIndex[i]
				prevLogIndex := nextIndex - 1

				// if the prev log index exists, send the term, otherwise garbage value
				var prevLogTerm int
				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				} else {
					prevLogTerm = 0
				}

				// copy the entries we need to send to the client (so we can unlock the mutex)
				entries := make([]LogEntry, 0)
				if nextIndex < len(rf.log) {
					entries = rf.log[nextIndex:]
				}

				args := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}

				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, args, reply)

				rf.mu.Lock()

				// after unlocking and sending, make sure we are still leader in the correct term
				if !ok || rf.role != LEADER || rf.currentTerm != currentTerm || rf.nextIndex[i] != nextIndex {
					rf.mu.Unlock()
					return
				}
				if rf.checkTermAndTransitionToFollower(true, reply.Term) {
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					// follower was able to append their entries!
					rf.matchIndex[i] = prevLogIndex + len(entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.updateCommitIndex()
					rf.mu.Unlock()
					return
				} else {
					if reply.TermNeeded != -1 {

						// if we have the term, jump back to the index of that term
						lastIndexOfConflictTerm := -1
						for j := len(rf.log) - 1; j >= 0; j-- {
							if rf.log[j].Term == reply.TermNeeded {
								lastIndexOfConflictTerm = j
								break
							}
						}
						if lastIndexOfConflictTerm != -1 {
							rf.nextIndex[i] = lastIndexOfConflictTerm + 1
						} else {
							// leader does not have that term, just jump to index
							rf.nextIndex[i] = reply.IndexNeeded
						}
					} else {
						// no term information, use the index
						rf.nextIndex[i] = reply.IndexNeeded
					}

				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

// if votes received (replies with true) is majority, become leader
// if AppendEntries RPC received from new leader, convert to follower
// if election timer elapses, start new election
func (rf *Raft) sendRequestVoteToAll() {
	rf.mu.Lock()
	if rf.role != CANDIDATE {
		rf.mu.Unlock()
		return
	}
	me := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	var wg sync.WaitGroup
	numPeers := len(rf.peers)
	rf.mu.Unlock()

	numVotesReceived := 1 // always vote for self
	numForConsensus := (numPeers / 2) + 1

	var voteMx sync.Mutex
	for i := 0; i < numPeers; i++ {
		if i == me {
			continue
		}
		wg.Add(1)
		go func(i int, numVotesReceived *int) {
			defer wg.Done()

			requestVoteReply := &RequestVoteReply{}
			rf.logger.Log(0, "Sending requestVote to %d in term %d", i, requestVoteArgs.Term)
			ok := rf.sendRequestVote(i, requestVoteArgs, requestVoteReply)

			rf.mu.Lock()

			// after locking + sending, ensure we are still in the correct term
			currentTerm := rf.currentTerm
			if rf.checkTermAndTransitionToFollower(true, requestVoteReply.Term) {
				rf.mu.Unlock()
				return
			}

			// after locking + sending, make sure we are still a candidate
			if rf.role != CANDIDATE || rf.currentTerm != requestVoteArgs.Term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			voteGranted := 0
			if requestVoteReply.VoteGranted {
				voteGranted = 1
			}
			rf.logger.Log(0, "reply from %d term %d, vote granted %d", i, currentTerm, voteGranted)

			if !ok {
				rf.logger.Log(0, "Node %d did not vote in the election for term %d", i, currentTerm)
			} else if requestVoteReply.Term > currentTerm {
				rf.logger.Log(0, "Node %d got term higher than current term %d. transitioning to %d, ", me, currentTerm, requestVoteReply.Term)
				rf.transitionToFollower(false, requestVoteReply.Term)
			} else if requestVoteReply.Term == currentTerm && requestVoteReply.VoteGranted {
				voteMx.Lock()
				*numVotesReceived++

				rf.logger.Log(0, "Node %d got vote for term %d", me, i)

				if *numVotesReceived == numForConsensus {
					// we have enough votes, become leader right now
					rf.logger.Log(0, "node %d elected as leader in term %d, with %d votes", me, currentTerm, *numVotesReceived)
					rf.transitionToLeader(false)
					//	go rf.sendAppendEntriesToFollowers() // immediately let everyone know we're leader
				}
				voteMx.Unlock()
			}
		}(i, &numVotesReceived)

	}
	wg.Wait()
}

func (rf *Raft) ticker() {
	rf.resetElectionTimer(false)
	var heartbeatTicker *time.Ticker

	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role

		// separate ticker for the heartbeat (so it can be fast!)
		if role == LEADER && heartbeatTicker == nil {
			heartbeatTicker = time.NewTicker(50 * time.Millisecond) // heartbeat every 50ms
			rf.logger.Log(0, "Starting heartbeat ticker")
		} else if role != LEADER && heartbeatTicker != nil {
			heartbeatTicker.Stop()
			heartbeatTicker = nil
			rf.logger.Log(0, "Stopping heartbeat ticker")
		}

		// chanel that stores if the election + heartbeat have fired
		electionChan := rf.electionTimer.C
		var hbChan <-chan time.Time
		if heartbeatTicker != nil {
			hbChan = heartbeatTicker.C
		}
		rf.mu.Unlock()

		select {
		case <-electionChan:
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.logger.Log(0, "Election timer fired, becoming candidate")
				rf.transitionToCandidate(true)
				rf.logger.Log(0, "Now candidate, sending request votes")
				go rf.sendRequestVoteToAll()
			}
			rf.resetElectionTimer(true)
			rf.mu.Unlock()

		case <-hbChan:
			rf.mu.Lock()
			if rf.role == LEADER {
				go rf.sendAppendEntriesToFollowers()
			}
			rf.mu.Unlock()

		// prevent cpu from spinning
		case <-time.After(10 * time.Millisecond):
		}

	}
}

func randomWait(duration int) {
	ms := 50 + (rand.Int63() % int64(duration))
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) transitionToCandidate(alreadyLocked bool) {
	if !alreadyLocked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	rf.resetElectionTimer(true)

}

func (rf *Raft) transitionToLeader(alreadyLocked bool) {
	if !alreadyLocked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	rf.role = LEADER
	rf.persist()

	lastLogIndex := len(rf.log)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex
		rf.matchIndex[i] = 0
	}

	rf.resetElectionTimer(true)
}

func (rf *Raft) checkTermAndTransitionToFollower(alreadyLocked bool, term int) bool {
	if !alreadyLocked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	if term > rf.currentTerm {
		rf.transitionToFollower(true, term)
		rf.votedFor = -1
		return true
	}

	return false
}

func (rf *Raft) transitionToFollower(alreadyLocked bool, term int) {
	if !alreadyLocked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	rf.role = FOLLOWER

	if term >= rf.currentTerm {
		rf.currentTerm = term
		rf.persist()
	}

	rf.resetElectionTimer(true)
}

func (rf *Raft) resetElectionTimer(alreadyLocked bool) {
	if !alreadyLocked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	// pre 1.23 go, we need to drain the channel since it is buffered...
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}

	}

	ms := ELECTION_TIMEOUT + time.Duration(rand.Int63()%int64(150*time.Millisecond))
	rf.electionTimer.Reset(ms)
	rf.logger.Log(0, "Election timer reset to %d", ms)
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
	rf.logger = logger.NewLogger(me+1, true, fmt.Sprintf("raft-%d", me), constants.RaftLoggingMap)

	// Your initialization code here (4A, 4B, 4C).
	rf.role = FOLLOWER
	rf.electionTimer = time.NewTimer(ELECTION_TIMEOUT)

	// persisted state (all)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	// volatile state (all)
	rf.commitIndex = -1
	rf.lastApplied = -1

	// volatile state (leader only)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.appendMutexes = make([]sync.Mutex, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logger.Log(constants.LogRaftStart, "Raft server started")

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.checkAndApply(applyCh)

	return rf
}
