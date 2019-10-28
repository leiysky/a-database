package storage

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var _ Storage = &KVStorage{}

type KVStorage struct {
	db *leveldb.DB
}

func NewKVStorage(cfg *Config) Storage {
	db, err := leveldb.OpenFile(cfg.Path, &opt.Options{})
	if err != nil {
		return nil
	}
	return &KVStorage{
		db: db,
	}
}

func (s *KVStorage) Get(k []byte) ([]byte, error) {
	return s.db.Get(k, nil)
}

func (s *KVStorage) Put(k, v []byte) error {
	return s.db.Put(k, v, nil)
}

func (s *KVStorage) Delete(k []byte) error {
	return s.db.Delete(k, nil)
}

func (s *KVStorage) Scan(low, up []byte) Iterator {
	return s.db.NewIterator(&util.Range{low, up}, nil)
}
