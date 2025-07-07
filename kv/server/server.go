package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	startTs := req.GetVersion()
	txn := mvcc.NewMvccTxn(reader, startTs)
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return nil, err
	}
	if lock == nil || lock.Ts > startTs {
		value, err := txn.GetValue(req.GetKey())
		if err != nil {
			return nil, err
		}
		if value == nil {
			return &kvrpcpb.GetResponse{
				NotFound: true,
			}, nil
		}
		return &kvrpcpb.GetResponse{
			Value: value,
		}, nil
	} else {
		lock_info := &kvrpcpb.LockInfo{
			Key:         req.GetKey(),
			PrimaryLock: lock.Primary,
			LockVersion: lock.Ts,
		}
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: lock_info,
			},
		}, nil
	}
}

func prewriteMutations(req *kvrpcpb.PrewriteRequest) [][]byte {
	mutations := make([][]byte, 0, len(req.Mutations))
	for _, mutation := range req.Mutations {
		mutations = append(mutations, mutation.Key)
	}
	return mutations
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(prewriteMutations(req))
	defer server.Latches.ReleaseLatches(prewriteMutations(req))

	startTs := req.GetStartVersion()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, startTs)
	var errors []*kvrpcpb.KeyError
	for _, mutation := range req.Mutations {
		write, lastCommitTs, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && lastCommitTs > startTs {
			errors = append(errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    startTs,
					ConflictTs: lastCommitTs,
					Key:        mutation.Key,
				},
			})
			continue
		}
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			errors = append(errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					Key:         mutation.Key,
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
				},
			})
			continue
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      startTs,
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		})
		if mvcc.WriteKindFromProto(mutation.Op) == mvcc.WriteKindPut {
			txn.PutValue(mutation.Key, mutation.Value)
		} else if mvcc.WriteKindFromProto(mutation.Op) == mvcc.WriteKindDelete {
			txn.DeleteValue(mutation.Key)
		}
	}

	if len(errors) == 0 {
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
	}

	return &kvrpcpb.PrewriteResponse{
		Errors: errors,
	}, nil
}

func commitKeys(req *kvrpcpb.CommitRequest) [][]byte {
	keys := make([][]byte, 0, len(req.Keys))
	for _, key := range req.Keys {
		keys = append(keys, key)
	}
	return keys
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(commitKeys(req))
	defer server.Latches.ReleaseLatches(commitKeys(req))

	startTs := req.GetStartVersion()
	commitTs := req.GetCommitVersion()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, commitTs)
	for _, key := range req.Keys {
		// Check if there's already a write record for this transaction
		// We need to create a transaction with startTs to properly search for existing writes
		write, _, err := mvcc.NewMvccTxn(reader, startTs).CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			// Transaction already committed or rolled back
			if write.Kind == mvcc.WriteKindRollback {
				// Transaction was rolled back
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "transaction was rolled back",
					},
				}, nil
			}
			// Transaction already committed, this is a repeat commit - just ignore
			continue
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// No lock and no write record - this means the transaction was never prewritten
			// According to Percolator protocol, this is treated as a successful no-op
			continue
		}
		if lock.Ts != startTs {
			// Lock belongs to a different transaction
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "lock conflict",
				},
			}, nil
		}

		// Lock belongs to this transaction, commit it
		txn.PutWrite(key, commitTs, &mvcc.Write{
			StartTS: startTs,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	// Write the transaction's modifications to storage
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
