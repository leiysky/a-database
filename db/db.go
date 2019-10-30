package db

import (
	"github.com/leiysky/a-database/executor"
	"github.com/leiysky/a-database/parser"
	"github.com/leiysky/a-database/storage"
)

type DB struct {
	store storage.Storage
}

func NewDB(store storage.Storage) *DB {
	return &DB{
		store: store,
	}
}

func (db *DB) ExecuteQuery(sql string) error {
	// Step 1: parse sql into ast
	parser := parser.New()
	node := parser.Parse(sql)

	// Step 2: compile ast into executor
	exec := executor.Compile(node)
	return nil
}
