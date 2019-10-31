package executor

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/leiysky/a-database/util"
	"github.com/xwb1989/sqlparser"
)

type Expression interface {
	Eval(*util.Row) interface{}

	EvalBool(*util.Row) bool
}

type baseExpression struct {
}

func (e *baseExpression) Eval(row *util.Row) interface{} {
	return nil
}

func (e *baseExpression) EvalBool(row *util.Row) bool {
	return false
}

type OpType byte

const (
	OpEq OpType = iota
	OpNe
	OpGt
	OpLt
	OpGe
	OpLe
)

type Comparison struct {
	baseExpression

	Op    OpType
	Left  Expression
	Right Expression
}

func (e *Comparison) Eval(row *util.Row) interface{} {
	return e.EvalBool(row)
}

func (e *Comparison) EvalBool(row *util.Row) bool {
	l := e.Left.Eval(row)
	r := e.Right.Eval(row)
	if reflect.TypeOf(l) != reflect.TypeOf(r) {
		return false
	}
	switch e.Op {
	case OpEq:
		return reflect.DeepEqual(l, r)
	case OpGt:
		if v, ok := l.(int); ok {
			return v > r.(int)
		} else if v, ok := l.(string); ok {
			return strings.Compare(v, r.(string)) > 0
		} else if v, ok := l.(int64); ok {
			return v > r.(int64)
		} else if v, ok := l.(uint); ok {
			return v > r.(uint)
		} else if v, ok := l.(uint64); ok {
			return v > r.(uint64)
		} else {
			return false
		}
	case OpGe:
		if v, ok := l.(int); ok {
			return v >= r.(int)
		} else if v, ok := l.(string); ok {
			return strings.Compare(v, r.(string)) >= 0
		} else if v, ok := l.(int64); ok {
			return v >= r.(int64)
		} else if v, ok := l.(uint); ok {
			return v >= r.(uint)
		} else if v, ok := l.(uint64); ok {
			return v >= r.(uint64)
		} else {
			return false
		}
	case OpLt:
		if v, ok := l.(int); ok {
			return v < r.(int)
		} else if v, ok := l.(string); ok {
			return strings.Compare(v, r.(string)) < 0
		} else if v, ok := l.(int64); ok {
			return v < r.(int64)
		} else if v, ok := l.(uint); ok {
			return v < r.(uint)
		} else if v, ok := l.(uint64); ok {
			return v < r.(uint64)
		} else {
			return false
		}
	case OpLe:
		if v, ok := l.(int); ok {
			return v <= r.(int)
		} else if v, ok := l.(string); ok {
			return strings.Compare(v, r.(string)) <= 0
		} else if v, ok := l.(int64); ok {
			return v <= r.(int64)
		} else if v, ok := l.(uint); ok {
			return v <= r.(uint)
		} else if v, ok := l.(uint64); ok {
			return v <= r.(uint64)
		} else {
			return false
		}
	default:
		return false
	}
}

type ColumnValue struct {
	baseExpression

	Name *sqlparser.ColName
}

func (e *ColumnValue) Eval(row *util.Row) interface{} {
	_, offset := row.Schema.GetColumnByName(e.Name.Name.Lowered())
	return row.Values[offset]
}

type SQLValue struct {
	baseExpression

	Val interface{}
}

func (e *SQLValue) Eval(row *util.Row) interface{} {
	return e.Val
}

func rewriteExpr(expr sqlparser.Expr) Expression {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return rewriteComparisonExpr(v)
	case *sqlparser.ColName:
		return rewriteColNameExpr(v)
	case *sqlparser.SQLVal:
		return rewriteSQLVal(v)
	default:
		return &baseExpression{}
	}
}

func rewriteComparisonExpr(expr *sqlparser.ComparisonExpr) Expression {
	e := &Comparison{}
	switch expr.Operator {
	case sqlparser.EqualStr:
		e.Op = OpEq
	case sqlparser.LessThanStr:
		e.Op = OpLt
	case sqlparser.GreaterThanStr:
		e.Op = OpGt
	case sqlparser.LessEqualStr:
		e.Op = OpLe
	case sqlparser.GreaterEqualStr:
		e.Op = OpGe
	default:
		return nil
	}
	e.Left = rewriteExpr(expr.Left)
	e.Right = rewriteExpr(expr.Right)
	return e
}

func rewriteColNameExpr(expr *sqlparser.ColName) Expression {
	return &ColumnValue{
		Name: expr,
	}
}

func rewriteSQLVal(expr *sqlparser.SQLVal) Expression {
	switch expr.Type {
	case sqlparser.IntVal:
		v, _ := strconv.Atoi(string(expr.Val))
		return &SQLValue{
			Val: v,
		}
	case sqlparser.StrVal:
		return &SQLValue{
			Val: string(expr.Val),
		}
	default:
		return nil
	}
}
