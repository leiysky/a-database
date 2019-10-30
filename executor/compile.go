package executor

import (
	"github.com/xwb1989/sqlparser"
)

func Compile(stmt sqlparser.Statement) Executor {
	return compile(stmt)
}

func compile(stmt sqlparser.Statement) Executor {
	switch v := stmt.(type) {
	case *sqlparser.Select:
		return compileSelectStmt(v)
	default:
		panic("Unknown AST")
	}
}

func compileSelectStmt(stmt *sqlparser.Select) Executor {
	for _, f := range stmt.SelectExprs {
		extractField(f)
	}
	return nil
}

func extractField(expr sqlparser.SelectExpr) []string {
	var fields []string
	// switch e := expr.(type) {
	// case *sqlparser.AliasedExpr:

	// default:
	// 	return nil
	// }
	return fields
}
