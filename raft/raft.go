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
	"sort"

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

func (p *Progress) maybeUpdate(n uint64) bool {
	updated := false
	if p.Match < n {
		p.Match = n
		updated = true
	}
	if p.Next < n+1 {
		p.Next = n + 1
	}
	return updated
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
	// number of ticks since it reached last electionTimeout
	electionElapsed int

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
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	prs := make(map[uint64]*Progress)
	for _, prID := range c.peers {
		prs[prID] = new(Progress)
	}
	if len(c.peers) == 0 {
		prs[c.ID] = new(Progress)
	}
	for _, node := range cs.Nodes {
		prs[node] = new(Progress)
	}
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Prs:              prs,
		RaftLog:          newLog(c.Storage),
	}
	if !IsEmptyHardState(hs) {
		r.RaftLog.committed = hs.Commit
		r.Term = hs.Term
		r.Vote = hs.Vote
	}
	r.becomeFollower(r.Term, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.id == to {
		return false
	}
	ents, err := r.RaftLog.Entries(r.Prs[to].Next, r.RaftLog.LastIndex()+1)
	if err != nil {
		panic(err)
	}
	ps := make([]*pb.Entry, 0, len(ents))
	for _, e := range ents {
		cloned := e
		ps = append(ps, &cloned)
	}
	logIdx := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(logIdx)
	if err != nil {
		panic(err)
	}
	r.send(pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, Entries: ps, LogTerm: logTerm, Index: logIdx, Commit: r.RaftLog.committed})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, Commit: commit})
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	random := r.electionTimeout + rand.Intn(r.electionTimeout)
	if r.electionElapsed > random {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term != r.Term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid state")
	}
	r.Vote = r.id
	r.Term = r.Term + 1
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid state")
	}
	r.Lead = r.id
	r.State = StateLeader
	for prID := range r.Prs {
		if prID == r.id {
			continue
		}
		r.Prs[prID].Match = 0
		r.Prs[prID].Next = r.RaftLog.LastIndex() + 1
	}
	r.appendEntry(pb.Entry{})
	r.updateCommitted()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None) // set the leader to None and wait to vote
		}
	case m.Term < r.Term:
		// TODO
		return nil
	}

	if m.MsgType == pb.MessageType_MsgRequestVote {
		canVote := r.Vote == m.From || // repeated vote
			(r.Vote == None && r.Lead == None) // not Vote
		if !r.RaftLog.isUpToDate(m.LogTerm, m.Index) {
			canVote = false
		}
		r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Reject: !canVote})
		if canVote {
			r.Vote = m.From
		}
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleMsgHup(m)
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From) // become to follower again
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			result := r.winVote()
			if result == 1 {
				r.becomeLeader()
				for prID := range r.Prs {
					r.sendAppend(prID)
				}
			} else if result == -1 {
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			r.bcastHeartbeat()
		case pb.MessageType_MsgPropose:
			for _, e := range m.Entries {
				r.appendEntry(*e)
			}
			if len(r.Prs) == 1 {
				// it's single node cluster
				r.updateCommitted()
			} else {
				for prID := range r.Prs {
					r.sendAppend(prID)
				}
			}
		case pb.MessageType_MsgAppend:
		case pb.MessageType_MsgAppendResponse:
			pr := r.Prs[m.From]
			if m.Reject {
				pr.Next = m.Index // pr.Next-1 will be sent in the next time
				r.sendAppend(m.From)
			} else {
				pr.maybeUpdate(m.Index)
				if r.updateCommitted() {
					// broadcast to make followers apply these logs
					for prID := range r.Prs {
						r.sendAppend(prID)
					}
				}
			}
		case pb.MessageType_MsgRequestVote:
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

func (r *Raft) majorityCommitted() uint64 {
	cs := make([]uint64, 0, len(r.Prs))
	for prID := range r.Prs {
		if prID == r.id {
			cs = append(cs, r.RaftLog.LastIndex())
		} else {
			cs = append(cs, r.Prs[prID].Match)
		}
	}
	sort.Slice(cs, func(i, j int) bool {
		return cs[i] > cs[j]
	})
	majority := len(r.Prs)/2 + 1
	return cs[majority-1] // index starts from 0
}

func (r *Raft) updateCommitted() bool {
	if r.State != StateLeader {
		panic("invalid state")
	}
	committed := r.majorityCommitted()
	return r.RaftLog.maybeCommit(committed, r.Term)
}

func (r *Raft) bcastHeartbeat() {
	for prID := range r.Prs {
		if prID == r.id {
			continue
		}
		r.sendHeartbeat(prID)
	}
}

func (r *Raft) handleMsgHup(m pb.Message) {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // vote to itself
	r.bcastVote()
	if r.winVote() == 1 { // it's a single node cluster
		r.becomeLeader()
	}
}

func (r *Raft) winVote() int {
	if r.State != StateCandidate {
		panic("invalid state")
	}
	accepted := 0
	rejected := 0
	for _, ok := range r.votes {
		if ok {
			accepted++
		} else {
			rejected++
		}
	}
	majority := len(r.Prs)/2 + 1
	if accepted >= majority {
		return 1
	} else if rejected >= majority {
		return -1
	}
	return 0
}

func (r *Raft) bcastVote() {
	for prID := range r.Prs {
		if prID == r.id {
			continue
		}
		logIdx := r.RaftLog.LastIndex()
		logTerm, err := r.RaftLog.Term(logIdx)
		if err != nil {
			panic(err)
		}
		r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVote, To: prID, Index: r.RaftLog.LastIndex(), LogTerm: logTerm})
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}
	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, e := range m.Entries {
		ents = append(ents, *e)
	}
	committed := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...)
	msg := pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex(), Reject: !committed}
	if !committed {
		msg.Index = m.Index
	}
	r.send(msg)
}

func (r *Raft) appendEntry(e pb.Entry) {
	li := r.RaftLog.LastIndex()
	e.Index = li + 1
	e.Term = r.Term
	r.RaftLog.append(e)
	r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) advance(rd Ready) {
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		r.RaftLog.applyTo(newApplied)
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(e.Index, e.Term)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
