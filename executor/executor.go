package executor

import (
	"fmt"

	"github.com/leiysky/a-database/context"
	"github.com/leiysky/a-database/util"

	"github.com/leiysky/a-database/storage"
	"github.com/xwb1989/sqlparser"
)

var (
	_ Executor = &Selection{}
	_ Executor = &Projection{}
	_ Executor = &Limit{}
	_ Executor = &TableScan{}
	_ Executor = &Insert{}
	_ Executor = &Join{}
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
	case *sqlparser.Show:
		return compileShow(v)
	default:
		panic("Unknown AST")
	}
}

func Exec(exec Executor, ctx context.Context) []*util.Row {
	exec.Open(ctx)
	var results []*util.Row
	for {
		r := exec.Next()
		if r == nil {
			break
		}
		results = append(results, r)
	}
	return results
}

type Executor interface {
	Next() *util.Row
	Open(context.Context)
	Close()
}

type baseExecutor struct {
	children []Executor
	ctx      context.Context
}

func (e *baseExecutor) Next() *util.Row {
	return nil
}

func (e *baseExecutor) Open(ctx context.Context) {
	e.ctx = ctx
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

	predicate Expression
}

func (e *Selection) Next() *util.Row {
	for {
		r := e.children[0].Next()
		if r == nil {
			return nil
		}
		if e.predicate.Eval(r) == true {
			return r
		}
	}
}

type Projection struct {
	baseExecutor

	// Project column from `from[i]` to `to[i]`
	from []*sqlparser.ColName
	to   []*sqlparser.ColName
}

func (e *Projection) Next() *util.Row {
	row := e.children[0].Next()
	if row == nil {
		return nil
	}
	schema := &util.Schema{
		TableName: row.Schema.TableName,
	}
	var cols []*util.Column
	var vals []interface{}
	for i, c := range e.from {
		col, offset := row.Schema.GetColumnByName(c.Name.Lowered())
		col.Name = e.to[i].Name.Lowered()
		cols = append(cols, &col)
		vals = append(vals, row.Values[offset])
	}
	schema.Columns = cols
	row.Values = vals
	row.Schema = schema
	return row
}

type TableScan struct {
	baseExecutor

	table  *sqlparser.TableName
	itr    storage.Iterator
	schema *util.Schema
}

func (e *TableScan) Open(ctx context.Context) {
	e.ctx = ctx
	prefix := []byte(e.table.Name.String() + ":")
	prefixBound := []byte(e.table.Name.String() + ":")
	prefixBound[len(prefixBound)-1]++
	e.itr = e.ctx.Store().Scan(prefix, prefixBound)
	e.schema = e.ctx.Schemas()[e.table.Name.String()]
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

func (e *Join) Open(ctx context.Context) {
	e.ctx = ctx
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

	TableName *sqlparser.TableName
	Keys      []string
	Values    []*util.Row
}

func (e *Insert) Open(ctx context.Context) {
	e.ctx = ctx
	schema := ctx.Schemas()[e.TableName.Name.String()]
	for _, r := range e.Values {
		r.Schema = schema
		e.Keys = append(e.Keys, util.GenerateKey(int64(r.Values[0].(int)), e.TableName.Name.String()))
	}

	b := util.NewRawBuilder()
	for i := range e.Keys {
		store := ctx.Store()
		store.Put([]byte(e.Keys[i]), BuildRaw(b, e.Values[i]))
		b.Reset()
	}
}

type ShowTables struct {
	baseExecutor

	tables chan string
	count  int
}

func (e *ShowTables) Open(ctx context.Context) {
	e.ctx = ctx
	e.tables = make(chan string, 1)
	for k := range e.ctx.Schemas() {
		e.tables <- k
		e.count++
	}
}

func (e *ShowTables) Next() *util.Row {
	if e.count == 0 {
		return nil
	}
	tableName, ok := <-e.tables
	if !ok {
		return nil
	}
	e.count--
	schema := &util.Schema{
		TableName: "",
		Columns: []*util.Column{{
			Type: util.ColumnFixedString,
			Name: "tables",
		}},
	}
	return &util.Row{
		Schema: schema,
		Values: []interface{}{tableName},
	}
}

func tryCast(v interface{}, tp util.ColumnType) interface{} {
	switch tp {
	case util.ColumnInt32:
		return castInt32(v)
	case util.ColumnInt64:
		return castInt64(v)
	case util.ColumnUInt32:
		return castUInt32(v)
	case util.ColumnUInt64:
		return castUInt64(v)
	default:
		return v
	}
}

func castInt32(v interface{}) int {
	switch value := v.(type) {
	case int:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case uint:
		return int(value)
	case uint32:
		return int(value)
	case uint64:
		return int(value)
	default:
		return 0
	}
}

func castInt64(v interface{}) int64 {
	switch value := v.(type) {
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return int64(value)
	case uint:
		return int64(value)
	case uint32:
		return int64(value)
	case uint64:
		return int64(value)
	default:
		return 0
	}
}

func castUInt32(v interface{}) uint {
	switch value := v.(type) {
	case int:
		return uint(value)
	case int32:
		return uint(value)
	case int64:
		return uint(value)
	case uint:
		return uint(value)
	case uint32:
		return uint(value)
	case uint64:
		return uint(value)
	default:
		return 0
	}
}

func castUInt64(v interface{}) uint64 {
	switch value := v.(type) {
	case int:
		return uint64(value)
	case int32:
		return uint64(value)
	case int64:
		return uint64(value)
	case uint:
		return uint64(value)
	case uint32:
		return uint64(value)
	case uint64:
		return uint64(value)
	default:
		return 0
	}
}

func BuildRaw(b *util.RawBuilder, row *util.Row) []byte {
	for i, c := range row.Schema.Columns {
		switch c.Type {
		case util.ColumnInt32:
			b.AppendInt32(tryCast(row.Values[i], util.ColumnInt32).(int))
		case util.ColumnInt64:
			b.AppendInt64(tryCast(row.Values[i], util.ColumnInt64).(int64))
		case util.ColumnUInt32:
			b.AppendUInt32(tryCast(row.Values[i], util.ColumnUInt32).(uint))
		case util.ColumnUInt64:
			b.AppendUInt64(tryCast(row.Values[i], util.ColumnUInt64).(uint64))
		case util.ColumnFixedString:
			b.AppendFixedString(tryCast(row.Values[i], util.ColumnFixedString).(string))
		case util.ColumnDate:
			b.AppendDate(tryCast(row.Values[i], util.ColumnDate).(util.Date))
		}
	}
	return b.Spawn()
}
