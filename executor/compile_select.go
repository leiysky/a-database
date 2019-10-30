package executor

import (
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func compileSelectStmt(stmt *sqlparser.Select) Executor {
	var cols []*sqlparser.ColName
	projection := &Projection{}
	for _, expr := range stmt.SelectExprs {
		if _, ok := expr.(*sqlparser.StarExpr); ok {
			projection = nil
			break
		}
		cols = append(cols, extractField(expr))
	}
	if projection != nil {
		projection.from = cols
		projection.to = cols
	}

	compileWhere(stmt.Where)

	compileLimit(stmt.Limit)

	compileFrom(stmt.From)

	return nil
}

func extractField(expr sqlparser.SelectExpr) *sqlparser.ColName {
	switch e := expr.(type) {
	case *sqlparser.AliasedExpr:
		colName := e.Expr.(*sqlparser.ColName)
		return colName
	default:
		panic("Unknown AST")
	}
}

func compileFrom(exprs sqlparser.TableExprs) Executor {
	var tableNames []*sqlparser.TableName
	for _, expr := range exprs {
		tableNames = append(tableNames, extractTableName(expr))
	}
	if len(tableNames) == 1 {
		return &TableScan{
			table: tableNames[0],
		}
	}
	join := &Join{}
	for i, name := range tableNames {
		ts := &TableScan{
			table: name,
		}
		join.children = append(join.children, ts)
		if i > 0 {
			newJoin := &Join{}
			newJoin.children = append(newJoin.children, join)
			join = newJoin
		}
	}
	return join
}

func compileWhere(exprs *sqlparser.Where) *Selection {
	return &Selection{}
}

func extractTableName(expr sqlparser.TableExpr) *sqlparser.TableName {
	alias := expr.(*sqlparser.AliasedTableExpr)
	name := alias.Expr.(sqlparser.SimpleTableExpr).(*sqlparser.TableName)
	return name
}

func compileLimit(limit *sqlparser.Limit) *Limit {
	val := limit.Rowcount.(*sqlparser.SQLVal)
	if val.Type == sqlparser.IntVal {
		v, _ := strconv.Atoi(string(val.Val))
		return &Limit{
			limit: v,
		}
	}
	panic("Unknown Limit")
}
