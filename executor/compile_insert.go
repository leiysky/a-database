package executor

import (
	"strconv"

	"github.com/leiysky/a-database/util"
	"github.com/xwb1989/sqlparser"
)

func compileInsert(stmt *sqlparser.Insert) Executor {
	var cols []*util.Column
	for _, c := range stmt.Columns {
		// Type will be filled in `Executor.Open`
		cols = append(cols, &util.Column{
			Name: c.String(),
		})
	}
	rows := extractInsertRows(stmt.Rows)
	rr := make([]*util.Row, len(rows))
	for i := range rows {
		row := &util.Row{
			Values: rows[i],
		}
		rr[i] = row
	}
	insert := &Insert{
		Values:    rr,
		TableName: &stmt.Table,
	}
	return insert
}

func extractInsertRows(rows sqlparser.InsertRows) [][]interface{} {
	var rr [][]interface{}
	values := rows.(sqlparser.Values)
	for _, tuple := range values {
		var row []interface{}
		for _, expr := range tuple {
			val := expr.(*sqlparser.SQLVal)
			switch val.Type {
			case sqlparser.IntVal:
				v, _ := strconv.Atoi(string(val.Val))
				row = append(row, v)
			case sqlparser.StrVal:
				row = append(row, string(val.Val))
			default:
				panic("Unkown value type")
			}
		}
		rr = append(rr, row)
	}
	return rr
}
