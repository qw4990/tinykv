package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	dbPath string
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		dbPath: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opt := badger.DefaultOptions
	opt.Dir = s.dbPath
	db, err := badger.Open(opt)
	s.db = db
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return storage.NewBadgerTxnStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) (err error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	defer func() {
		if err == nil {
			err = txn.Commit()
		} else {
			txn.Discard()
		}
	}()
	for _, modify := range batch {
		cf := modify.Cf()
		key := modify.Key()
		val := modify.Value()
		if val == nil { // delete
			if err = txn.Delete(engine_util.KeyWithCF(cf, key)); err != nil {
				return
			}
		} else { // put
			if err = txn.Set(engine_util.KeyWithCF(cf, key), val); err != nil {
				return
			}
		}
	}
	return
}
