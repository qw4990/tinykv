// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	if err := c.validate(); err != nil {
		panic(err)
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog := newLog(c.Storage)
	lastIndex := raftLog.LastIndex()

	prs := make(map[uint64]*Progress)
	if len(c.peers) > 0 {
		for _, id := range c.peers {
			prs[id] = &Progress{
				Match: 0,
				Next:  lastIndex + 1,
			}
		}
	} else {
		for _, id := range cs.Nodes {
			prs[id] = &Progress{}
		}
	}
	r := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             []pb.Message{},
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: 0,
	}
	if c.Applied > 0 {
		r.RaftLog.appliedTo(c.Applied)
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr, ok := r.Prs[to]
	if !ok {
		log.Infof("[sendAppend] Node %d not found in Peers", to)
		return false
	}

	nextIndex := pr.Next
	prevIndex := nextIndex - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)

	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return true
		}
		return false
	}

	entries, _ := r.RaftLog.EntriesFrom(nextIndex)

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: entriesToPointers(entries), // 可能是空的
		Commit:  r.RaftLog.committed,
	}

	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.id == to || r.State != StateLeader {
		return
	}
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed),
	}
	r.send(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.leadTransferee = None
			r.Step(pb.Message{
				Term:    r.Term,
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
				Commit:  r.RaftLog.committed,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				Term:    r.Term,
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
				Commit:  r.RaftLog.committed,
			})

			r.heartbeatElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.State = StateFollower
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = 0
	r.State = StateCandidate
	r.Lead = None
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = 0

	// NOTE: Leader should propose a noop entry on its term
	entry := pb.Entry{
		Term:      r.Term,
		EntryType: pb.EntryType_EntryNormal,
	}
	r.appendEntry(&entry)

	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		if id == r.id {
			r.updateProgress(id, lastIndex, lastIndex+1)
		} else {
			r.updateProgress(id, 0, lastIndex)
		}
	}
	// 广播 AppendEntries 消息给所有 followers
	if !r.brocAppend() {
		panic("failed to broadcast append entries")
	}
	r.PendingConfIndex = r.RaftLog.LastIndex() // 设置 PendingConfIndex 为当前日志的最后索引
	if len(r.Prs) == 1 {
		r.maybeCommit()
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {

	// local message
	case pb.MessageType_MsgHup:
		r.StartElection()

	case pb.MessageType_MsgBeat:
		r.SendHeartbeat()

	case pb.MessageType_MsgPropose:
		r.HandlePropose(m)

	// remote / network message
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResp(m)

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)

	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

		// case pb.MessageType_MsgTransferLeader:
		// 	r.handleTransferLeader(m)

		// case pb.MessageType_MsgTimeoutNow:
		// 	r.handleTransferLeaderTimeout()
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.RaftLog.pendingSnapshot != nil {
		return
	}
	if m.Term < r.Term { // the message is outdated, reject this request
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
			Index:   r.RaftLog.LastIndex(),
		})
		return
	}
	if m.Term > r.Term || // follow the new leader
		(r.State != StateLeader) { // update follower info
		r.becomeFollower(m.Term, m.From)
	}
	if r.State == StateLeader { // only follower or candidate need to handle append requests
		return
	}

	ok, conflictIndex, conflictTerm := r.checkLogMatching(m.Index, m.LogTerm)
	if !ok {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
			Index:   conflictIndex,
			LogTerm: conflictTerm,
		})
		return
	}

	r.append_or_rewriteEntry(m.Index, m.Entries)

	if m.Commit > r.RaftLog.committed {
		matchIndex := m.Index + uint64(len(m.Entries))
		newCommit := min(m.Commit, matchIndex)
		r.RaftLog.committed = newCommit
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		// if the term of the heartbeat is less than current term, reject it
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	r.electionElapsed = 0
	r.Lead = m.From

	// advance the commit index
	if m.Commit > r.RaftLog.committed {
		matchIndex := m.Index + uint64(len(m.Entries))
		r.RaftLog.committed = min(m.Commit, matchIndex)
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if r.RaftLog.pendingSnapshot != nil {
		return
	}
	snapshot := m.Snapshot
	if snapshot == nil || snapshot.Metadata == nil {
		return
	}

	snapshotIndex := snapshot.Metadata.Index
	snapshotTerm := snapshot.Metadata.Term

	if r.RaftLog.committed >= snapshotIndex {
		return
	}

	// 转为 follower
	r.becomeFollower(snapshotTerm, m.From)

	// 应用 snapshot
	r.RaftLog.pendingSnapshot = snapshot
	r.RaftLog.committed = snapshotIndex
	r.RaftLog.applied = snapshotIndex
	r.RaftLog.stabled = snapshotIndex
	r.RaftLog.entries = nil

	r.Prs = make(map[uint64]*Progress)
	for _, id := range snapshot.Metadata.ConfState.Nodes {
		r.Prs[id] = &Progress{}
	}

	// 设置自己 Match 和 Next
	if pr, ok := r.Prs[r.id]; ok {
		pr.Match = snapshotIndex
		pr.Next = snapshotIndex + 1
	}

	// 响应 Leader
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   snapshotIndex,
		Reject:  false,
	}
	r.send(resp)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) StartElection() {
	if r.State == StateLeader {
		return
	}
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		// if there is only one node in the cluster, it can become leader immediately
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	// 1. 发送 RequestVote 消息
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: lastTerm,
			Index:   lastIndex,
		}
		r.send(m)
	}
}

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) SendHeartbeat() {
	if r.State != StateLeader {
		return
	}
	for id := range r.Prs {
		r.sendHeartbeat(id)
	}
}

// requests votes for election.
func (r *Raft) handleRequestVote(m pb.Message) {
	// the candidate's term must be at least as large as the current term
	if m.Term < r.Term {
		r.send(rejectMsg(m, r.Term))
		return
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	if r.Vote == None || r.Vote == m.From {
		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
		upToDate := m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex)

		if upToDate {
			r.Vote = m.From
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  false,
			})
			return
		}
	}

	// reject the vote request
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	})
}

func rejectMsg(m pb.Message, term uint64) pb.Message {
	reject := pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    term,
		MsgType: m.MsgType,
		Reject:  true,
	}
	return reject
}

func (r *Raft) handleVoteResp(m pb.Message) {
	if r.State != StateCandidate {
		return
	}
	// if the responsor's term is greater than the current term,
	// step down to follower and stop the election
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	// r is not a candidate now
	if m.Term < r.Term || r.State != StateCandidate {
		return
	}

	// count the votes
	r.votes[m.From] = !m.Reject
	voteCount := 0
	denialCount := 0
	for _, vote := range r.votes {
		if vote {
			voteCount++
		} else {
			denialCount++
		}
	}

	quorum := len(r.Prs) / 2
	if voteCount > quorum {
		r.becomeLeader()
		return
	}

	if denialCount > quorum {
		r.becomeFollower(r.Term, None)
		return
	}
}

func (r *Raft) appendEntry(entries ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for _, ent := range entries {
		ent.Index = lastIndex + 1
		lastIndex++
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}
}

func (r *Raft) updateProgress(id uint64, match, next uint64) {
	pr, ok := r.Prs[id]
	if !ok {
		pr = &Progress{}
		r.Prs[id] = pr
	}
	pr.Match = match
	pr.Next = next
}

func (r *Raft) brocAppend() bool {
	// 'bcastAppend' is a broadcast function that sends 'MessageType_MsgAppend' to all peers.
	// It is called by the leader to replicate logs to followers.
	if r.State != StateLeader {
		return false
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		if !r.sendAppend(id) {
			return false
		}
	}
	return true
}

func (r *Raft) maybeCommit() bool {
	mci := r.RaftLog.maybeCommit(r.Prs, r.Term)
	if mci > r.RaftLog.committed {
		r.RaftLog.commitTo(mci)
		r.brocAppend()
		return true
	}
	return false
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil || snapshot.Metadata == nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}

	r.send(msg)
	// avoid snapshot is sent too frequently
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// checkLogMatching check if the log at index matches the term.
// If it conflicts, it returns false and provides conflict information for fast fallback.
func (r *Raft) checkLogMatching(index, term uint64) (ok bool, conflictIndex uint64, conflictTerm uint64) {
	lastIndex := r.RaftLog.LastIndex()
	if index > lastIndex {
		return false, lastIndex + 1, 0
	}

	localTerm, err := r.RaftLog.Term(index)
	if err != nil {
		return false, index, 0
	}

	if localTerm != term && localTerm != 0 {
		conflictTerm := localTerm
		conflictIndex := r.RaftLog.findFirstIndexOfTerm(conflictTerm)
		return false, conflictIndex, conflictTerm
	}
	return true, 0, 0
}

func (l *RaftLog) findFirstIndexOfTerm(term uint64) uint64 {
	first := l.FirstIndex()
	for i := l.LastIndex(); i >= first; i-- {
		t, err := l.Term(i)
		if err != nil {
			break
		}
		if t == term {
		} else if t < term {
			return i + 1
		}
	}
	return first
}

func (r *Raft) append_or_rewriteEntry(prevLogIndex uint64, entries []*pb.Entry) {
	fromIndex := prevLogIndex + 1
	log := r.RaftLog
	for i, entry := range entries {
		localIndex := fromIndex + uint64(i)
		if localIndex <= log.LastIndex() {
			localTerm, _ := log.Term(localIndex)
			if localTerm != entry.Term {
				log.entries = log.entries[:localIndex-log.entries[0].Index]
				if log.stabled >= localIndex {
					log.stabled = localIndex - 1
				}
				r.appendEntry(entries[i:]...)
				return
			}
		} else {
			if log.stabled >= localIndex {
				log.stabled = localIndex - 1
			}
			r.appendEntry(entries[i:]...)
			return
		}
	}
}

func (r *Raft) handleHeartbeatResp(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Commit < r.RaftLog.committed || r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) HandlePropose(m pb.Message) {
	if r.State != StateLeader { // only leader can accept proposes from clients
		return
	}
	if r.leadTransferee != None {
		return
	}
	if m.Term < r.Term {
		for _, ent := range m.Entries {
			ent.Term = r.Term
		}
	}
	r.appendEntry(m.Entries...) // append entries to the local log
	lastIndex := r.RaftLog.LastIndex()
	r.updateProgress(r.id, lastIndex, lastIndex+1) // update leader's progress
	if !r.brocAppend() {                           // trigger append request to followers
		panic(ErrProposalDropped)
	}
	if len(r.Prs) == 1 { // if this is a single node cluster, push commit immediately
		r.maybeCommit()
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	pr, ok := r.Prs[m.From]
	if !ok { // invalid node number
		return
	}

	if m.Reject {
		if m.Index > 0 { // fast fallback
			pr.Next = m.Index
		} else if pr.Next > 1 { // fallback to the previous index
			pr.Next--
		}
		r.sendAppend(m.From)
		return
	}

	// update the progress
	pr.Match = m.Index
	pr.Next = pr.Match + 1

	r.maybeCommit() // try to promote the commit index
	if r.leadTransferee == m.From && pr.Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgTimeoutNow,
		})
		r.leadTransferee = None
	}
}
