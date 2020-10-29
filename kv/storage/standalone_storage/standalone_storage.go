package standalone_storage

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	path string
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		path: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.path
	opts.ValueDir = s.path
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	log.Info("StandAloneStorage started")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.db == nil {
		return fmt.Errorf("db already closed")
	}
	err := s.db.Close()
	if err == nil {
		s.db = nil
		log.Info("StandAloneStorage stopped")
		return nil
	}
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// start a read-only transaction to get a consistent view of the db
	return &Reader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch data := m.Data.(type) {
			case storage.Put:
				composed_key := append([]byte(data.Cf+"_"), m.Key()...)
				if err := txn.Set(composed_key, m.Value()); err != nil {
					return err
				}
			case storage.Delete:
				composed_key := append([]byte(data.Cf+"_"), m.Key()...)
				if err := txn.Delete(composed_key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

type Reader struct {
	txn *badger.Txn
}

func (r *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	compsed_key := append([]byte(cf+"_"), key...)
	item, err := r.txn.Get(compsed_key)
	switch err {
	case badger.ErrKeyNotFound:
		return nil, nil
	case nil:
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		return val, err
	default:
		return nil, err
	}
}

func (r *Reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *Reader) Close() {
	if err := r.txn.Commit(); err != nil {
		log.Fatal("failed to commit a read only transaction")
	}
}
