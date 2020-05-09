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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	first, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	last, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// put all entries into raftLog.entries to simplify its implementation
	entries, err := storage.Entries(first, last+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:   storage,
		committed: first - 1,
		applied:   first - 1,
		stabled:   last,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	first := l.FirstIndex()
	if l.stabled+1 < first {
		panic(fmt.Sprintf("invalid stabled state %v %v", l.stabled, first))
	}
	return l.entries[l.stabled-first+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents, err := l.Entries(l.applied+1, l.committed+1)
	if err != nil {
		panic(err)
	}
	return ents
}

func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {
	// NOTE: all entries are stored in RaftLog.entries
	first := l.FirstIndex()
	if lo < first || hi < first {
		return nil, fmt.Errorf("invalid range lo=%v, hi=%v, first=%v", lo, hi, first)
	}
	if hi > l.LastIndex()+1 {
		hi = l.LastIndex() + 1
	}
	return l.entries[lo-first : hi-first], nil
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	ents, err := l.Entries(i, i+1)
	if err != nil {
		return 0, err
	}
	if len(ents) != 1 {
		return 0, fmt.Errorf("invalid log index %v", i)
	}
	return ents[0].Term, nil
}

func (l *RaftLog) isUpToDate(term, index uint64) bool {
	li := l.LastIndex()
	lastTerm, err := l.Term(li)
	if err != nil {
		panic(err)
	}
	return term > lastTerm || (lastTerm == term && index >= li)
}

func (l *RaftLog) commitTo(tocommit uint64) bool {
	if l.committed < tocommit {
		l.committed = tocommit
		return true
	}
	return false
}

func (l *RaftLog) mustAppend(ent pb.Entry) {
	l.entries = append(l.entries, ent)
}

func (l *RaftLog) maybeAppend(index, term, committed uint64, ents ...pb.Entry) bool {
	// resolve conflicts
	it, err := l.Term(index)
	if err != nil {
		return false
	}
	if it != term {
		return false
	}
	if len(ents) == 0 {
		return true
	}
	first := ents[0].Index
	if first > l.LastIndex()+1 || first <= l.stabled {
		return false
	}
	if len(l.entries) == 0 {
		l.entries = append(l.entries, ents...)
		l.commitTo(min(committed, l.LastIndex()))
		return true
	}
	p := l.entries[0].Index
	l.entries = l.entries[:first-p]
	l.entries = append(l.entries, ents...)
	l.commitTo(min(committed, l.LastIndex()))
	return true
}
