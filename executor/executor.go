package executor

import (
	"fmt"

	"github.com/leiysky/a-database/context"
	"github.com/leiysky/a-database/util"

	"github.com/leiysky/a-database/storage"
	"github.com/xwb1989/sqlparser"
)

func Compile(stmt sqlparser.Statement) Executor {
	return compile(stmt)
}

func compile(stmt sqlparser.Statement) Executor {
	switch v := stmt.(type) {
	case *sqlparser.Select:
		return compileSelectStmt(v)
	case *sqlparser.Insert:
		return compileInsert(v)
	default:
		panic("Unknown AST")
	}
}

type Executor interface {
	Next() *util.Row
	Open(*context.Context)
	Close()
}

type baseExecutor struct {
	children []Executor
	ctx      *context.Context
}

func (e *baseExecutor) Next() *util.Row {
	return nil
}

func (e *baseExecutor) Open(ctx *context.Context) {
	for _, c := range e.children {
		c.Open(ctx)
	}
}

func (e *baseExecutor) Close() {
	for _, c := range e.children {
		c.Close()
	}
}

type Selection struct {
	baseExecutor

	predicate sqlparser.Expr
}

func (e *Selection) Next() *util.Row {
	for {
		r := e.children[0].Next()
		if r == nil {
			return nil
		}
		if eval(r, e.predicate) {
			return r
		}
	}
}

func eval(row *util.Row, predicate sqlparser.Expr) bool {
	return true
}

type Projection struct {
	baseExecutor

	// Project column from `from[i]` to `to[i]`
	from []*sqlparser.ColName
	to   []*sqlparser.ColName
}

type TableScan struct {
	baseExecutor

	table  *sqlparser.TableName
	itr    storage.Iterator
	schema *util.Schema
}

func (e *TableScan) Open(ctx *context.Context) {
	e.ctx = ctx
	e.itr = e.ctx.Store.ScanAll()
	e.schema = e.ctx.Schemas[e.table.Name.String()]
	if e.schema == nil {
		panic("Invalid context")
	}
}

func (e *TableScan) Next() *util.Row {
	if e.itr.Next() {
		raw := e.itr.Value()
		return util.ReadRow(raw, e.schema)
	}
	return nil
}

type Limit struct {
	baseExecutor

	limit int
	count int
}

func (e *Limit) Next() *util.Row {
	if e.count >= e.limit {
		return nil
	}
	r := e.children[0].Next()
	e.count++
	return r
}

type Join struct {
	baseExecutor

	rowBuff []*util.Row
}

func (e *Join) Open(ctx *context.Context) {
	for _, c := range e.children {
		c.Open(ctx)
	}

	var left []*util.Row
	for {
		r := e.children[0].Next()
		if r == nil {
			break
		}
		left = append(left, r)
	}

	var right []*util.Row
	for {
		r := e.children[1].Next()
		if r == nil {
			break
		}
		right = append(right, r)
	}

	for _, rl := range left {
		for _, rr := range right {
			e.rowBuff = append(e.rowBuff, join(rl, rr))
		}
	}
}

func join(l, r *util.Row) *util.Row {
	newSchema := &util.Schema{
		TableName: fmt.Sprintf("(%s,%s)", l.Schema.TableName, r.Schema.TableName),
	}
	var newCols []*util.Column
	for _, c := range l.Schema.Columns {
		newC := &util.Column{
			Type:   c.Type,
			Name:   fmt.Sprintf("%s.%s", l.Schema.TableName, c.Name),
			Strlen: c.Strlen,
		}
		newCols = append(newCols, newC)
	}

	for _, c := range r.Schema.Columns {
		newC := &util.Column{
			Type:   c.Type,
			Name:   fmt.Sprintf("%s.%s", r.Schema.TableName, c.Name),
			Strlen: c.Strlen,
		}
		newCols = append(newCols, newC)
	}
	newSchema.Columns = newCols

	return &util.Row{
		Schema: newSchema,
		Values: append(l.Values, r.Values...),
	}
}

type Insert struct {
	baseExecutor

	Keys   []string
	Values []*util.Row
}
