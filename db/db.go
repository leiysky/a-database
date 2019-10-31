package db

import (
	"github.com/leiysky/a-database/context"
	"github.com/leiysky/a-database/util"

	"github.com/leiysky/a-database/executor"
	"github.com/leiysky/a-database/parser"
)

type DB struct {
	ctx context.Context
}

func NewDB(ctx context.Context) *DB {
	return &DB{
		ctx: ctx,
	}
}

func (db *DB) ExecuteQuery(sql string) []*util.Row {
	// Step 1: parse sql into ast
	parser := parser.New()
	stmt := parser.Parse(sql)

	// Step 2: compile ast into executor
	exec := executor.Compile(stmt)

	// Step 3: execute query
	return executor.Exec(exec, db.ctx)
}
