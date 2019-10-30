package storage

import "github.com/syndtr/goleveldb/leveldb/iterator"

type Storage interface {
	Get([]byte) ([]byte, error)
	Put([]byte, []byte) error
	Delete([]byte) error
	Scan([]byte, []byte) Iterator
	ScanAll() Iterator
}

type Iterator interface {
	iterator.Iterator
}
