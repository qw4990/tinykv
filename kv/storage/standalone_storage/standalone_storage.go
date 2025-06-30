package standalone_storage

import (
	"errors"

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
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	if s.db != nil {
		return nil // Already started
	}
	s.db = engine_util.CreateDB(s.conf.DBPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StorageReaderImpl{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch data := modify.Data.(type) {
		case storage.Put:
			return engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
		case storage.Delete:
			return engine_util.DeleteCF(s.db, data.Cf, data.Key)
		}
	}
	return errors.New("unsupported modify type in StandAloneStorage Write")
}

type StorageReaderImpl struct {
	txn *badger.Txn
}

func (r *StorageReaderImpl) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return val, err
}

func (r *StorageReaderImpl) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StorageReaderImpl) Close() {
	if r.txn != nil {
		r.txn.Discard()
	}
}
