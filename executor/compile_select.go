package executor

import (
	"strconv"

	"github.com/xwb1989/sqlparser"
)

func compileSelectStmt(stmt *sqlparser.Select) Executor {
	var exec Executor

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

	datasource := compileFrom(stmt.From)

	exec = datasource

	if stmt.Where != nil {
		selection := compileWhere(stmt.Where)
		selection.children = append(selection.children, exec)
		exec = selection
	}

	if stmt.Limit != nil {
		limit := compileLimit(stmt.Limit)
		limit.children = append(limit.children, exec)
		exec = limit
	}

	if projection != nil {
		projection.children = append(projection.children, exec)
		exec = projection
	}

	return exec
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

func compileWhere(expr *sqlparser.Where) *Selection {
	e := rewriteExpr(expr.Expr)
	return &Selection{
		predicate: e,
	}
}

func extractTableName(expr sqlparser.TableExpr) *sqlparser.TableName {
	alias := expr.(*sqlparser.AliasedTableExpr)
	switch name := alias.Expr.(type) {
	case sqlparser.TableName:
		return &name
	}
	return nil
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

func compileShow(show *sqlparser.Show) Executor {
	if show.Type == "tables" {
		return &ShowTables{}
	}
	return nil
}
