package executor

import (
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
	switch e.Op {
	case OpEq:
		result := tryCompare(l, r)
		if result == 0 {
			return true
		}
		return false
	case OpNe:
		result := tryCompare(l, r)
		if result != 0 && result != -2 {
			return true
		}
		return false
	case OpGt:
		result := tryCompare(l, r)
		if result > 0 {
			return true
		}
		return false
	case OpGe:
		result := tryCompare(l, r)
		if result >= 0 {
			return true
		}
		return false
	case OpLt:
		result := tryCompare(l, r)
		if result == -1 {
			return true
		}
		return false
	case OpLe:
		result := tryCompare(l, r)
		if result == 0 || result == -1 {
			return true
		}
		return false
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
	case sqlparser.NotEqualStr:
		e.Op = OpNe
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

func tryCompare(l, r interface{}) int {
	switch l.(type) {
	case int, int64:
		lv := tryCast(l, util.ColumnInt64).(int64)
		rv := tryCast(r, util.ColumnInt64).(int64)
		if lv > rv {
			return 1
		} else if lv == rv {
			return 0
		} else {
			return -1
		}
	case uint, uint64:
		lv := tryCast(l, util.ColumnUInt64).(uint64)
		rv := tryCast(r, util.ColumnUInt64).(uint64)
		if lv > rv {
			return 1
		} else if lv == rv {
			return 0
		} else {
			return -1
		}
	case string:
		lv := tryCast(l, util.ColumnFixedString).(string)
		rv := tryCast(r, util.ColumnFixedString).(string)
		return strings.Compare(lv, rv)
	case util.Date:
		lv := tryCast(l, util.ColumnDate).(util.Date).Timestamp()
		rv := tryCast(r, util.ColumnDate).(util.Date).Timestamp()
		if lv > rv {
			return 1
		} else if lv == rv {
			return 0
		} else {
			return -1
		}
	default:
		return -2
	}
}
