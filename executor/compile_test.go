package executor

import (
	"testing"

	"github.com/leiysky/a-database/parser"
	"github.com/leiysky/a-database/util"
	"github.com/leiysky/go-utils/assert"
	"github.com/xwb1989/sqlparser"
)

func TestCompileSelect(t *testing.T) {
	p := parser.New()
	stmt := p.Parse(`select a, * from b where 1 = 1.1 limit 1`)
	Compile(stmt)
}

func TestCompileInsert(t *testing.T) {
	p := parser.New()
	stmt := p.Parse(`insert into a(c1, c2) values (1, 2)`)
	Compile(stmt)
}

func TestExpression(t *testing.T) {
	assert := assert.New(t)

	expr := &Comparison{
		Op: OpEq,
		Left: &ColumnValue{
			Name: &sqlparser.ColName{
				Name: sqlparser.NewColIdent("c"),
			},
		},
		Right: &SQLValue{
			Val: 123,
		},
	}

	schema := &util.Schema{
		Columns: []*util.Column{
			{
				Name: "c",
				Type: util.ColumnInt32,
			},
		},
	}

	row := &util.Row{
		Schema: schema,
		Values: []interface{}{
			123,
		},
	}

	assert.Equal(expr.Eval(row), true)

	{
		sql := "select * from a where c = 123"
		stmt := parser.New().Parse(sql)
		expr := rewriteExpr(stmt.(*sqlparser.Select).Where.Expr)
		assert.Equal(expr.Eval(row), true)
	}
}
