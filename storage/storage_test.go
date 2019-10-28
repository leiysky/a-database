package storage

import (
	"os"
	"testing"

	"github.com/leiysky/go-utils/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

var config *Config

func SetUp() {
	config = &Config{
		Path: "tmp-data",
	}
}

func TearDown() {
	os.RemoveAll("tmp-data")
}

func TestStorage(t *testing.T) {
	SetUp()
	defer TearDown()
	assert := assert.New(t)
	s := NewKVStorage(config)

	s.Put([]byte("k1"), []byte("v1"))
	s.Put([]byte("k2"), []byte("v2"))

	v1, _ := s.Get([]byte("k1"))
	assert.Equal(v1, []byte("v1"))

	s.Delete([]byte("k1"))

	v1, err := s.Get([]byte("k1"))
	assert.Equal(err, leveldb.ErrNotFound)
}

func TestScan(t *testing.T) {
	SetUp()
	defer TearDown()
	assert := assert.New(t)

	s := NewKVStorage(config)
	s.Put([]byte{1}, []byte{1})
	s.Put([]byte{2}, []byte{2})
	s.Put([]byte{3}, []byte{3})

	itr := s.Scan([]byte{1}, []byte{3})

	for i := 1; i < 3; i++ {
		itr.Next()
		assert.True(itr.Valid())
		assert.Equal(itr.Key(), []byte{byte(i)})
		assert.Equal(itr.Value(), []byte{byte(i)})
	}
	assert.False(itr.Next())
	assert.False(itr.Valid())
}
