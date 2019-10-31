package util

import (
	"bytes"
	"strconv"
)

func Prettify(rows []*Row) string {
	if len(rows) == 0 {
		return ""
	}
	s := rows[0].Schema

	var cols []string
	var align []int
	for _, c := range s.Columns {
		cols = append(cols, c.Name)
		// fill spaces before and behind column name
		align = append(align, len(c.Name)+2)
	}

	var table [][]string

	table = append(table, cols)

	for _, r := range rows {
		var row []string
		for i, c := range r.Values {
			switch v := c.(type) {
			case Int32:
				row = append(row, strconv.Itoa(v))
			case Int64:
				row = append(row, strconv.FormatInt(v, 10))
			case UInt32:
				row = append(row, strconv.FormatUint(uint64(v), 10))
			case UInt64:
				row = append(row, strconv.FormatUint(uint64(v), 10))
			case FixedString:
				row = append(row, v)
			default:
				panic("Unknown type")
			}
			if len(row[i])+2 > align[i] {
				align[i] = len(row[i]) + 2
			}
		}
		table = append(table, row)
	}

	buf := bytes.NewBufferString("╔")

	for i, a := range align {
		for i := 0; i < a; i++ {
			buf.WriteString("═")
		}
		if i != len(align)-1 {
			buf.WriteString("╦")
		}
	}

	buf.WriteString("╗\n")

	writeLine := func() {
		buf.WriteString("╠")
		for i, a := range align {
			buf.WriteString("═")
			for i := 0; i < a; i++ {
				if i < a-1 {
					buf.WriteString("═")
				}
			}
			if i != len(align)-1 {
				buf.WriteString("╬")
			}
		}
		buf.WriteString("╣\n")
	}

	for i, r := range table {
		if i > 0 {
			writeLine()
		}
		buf.WriteString("║")
		for i, c := range r {
			buf.WriteString(" ")
			buf.WriteString(c)
			buf.WriteString(" ")
			for j := 0; j < align[i]-2-len(c); j++ {
				buf.WriteString(" ")
			}
			buf.WriteString("║")
		}
		buf.WriteString("\n")
	}

	buf.WriteString("╚")
	for i, a := range align {
		for i := 0; i < a; i++ {
			buf.WriteString("═")
		}
		if i != len(align)-1 {
			buf.WriteString("╩")
		}
	}
	buf.WriteString("╝\n")
	return buf.String()
}
