package util

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"
)

type ColumnType byte

func (tp ColumnType) String() string {
	switch tp {
	case ColumnInt32:
		return "Int32"
	case ColumnInt64:
		return "Int64"
	case ColumnUInt32:
		return "UInt32"
	case ColumnUInt64:
		return "UInt64"
	case ColumnFixedString:
		return "FixedString"
	case ColumnDate:
		return "Date"
	default:
		panic("Unknown ColumnType")
	}
}

const (
	ColumnInt32 ColumnType = iota
	ColumnInt64
	ColumnUInt32
	ColumnUInt64
	ColumnFixedString
	ColumnDate
)

type Int32 = int

type Int64 = int64

type UInt32 = uint

type UInt64 = uint64

type FixedString = string

func WhichColumnType(tp string) ColumnType {
	switch tp {
	case "Int32":
		return ColumnInt32
	case "Int64":
		return ColumnInt64
	case "UInt32":
		return ColumnUInt32
	case "UInt64":
		return ColumnUInt64
	case "FixedString":
		return ColumnFixedString
	case "Date":
		return ColumnDate
	default:
		panic("Unknown ColumnType")
	}
}

type Date time.Time

func (v Date) TypeName() string {
	return "Date"
}

func (dt Date) String() string {
	return time.Time(dt).Format(time.RFC3339)
}

// Timestamp will return Unix seconds timestamp
func (dt Date) Timestamp() int64 {
	return time.Time(dt).Unix()
}

type Schema struct {
	TableName string
	Columns   []*Column
}

// A schema file example: $PROJECT_ROOT/examples/schema/my_table
func (s *Schema) String() string {
	b := strings.Builder{}
	for _, c := range s.Columns {
		b.WriteString(c.String() + "\n")
	}
	return b.String()
}

func (s *Schema) GetColumnByName(name string) (col Column, offset int) {
	for i, c := range s.Columns {
		if c.Name == name {
			return *c, i
		}
	}
	return col, -1
}

type Column struct {
	Type ColumnType

	Name string
	// For FixedString
	Strlen int
}

func (c *Column) String() string {
	b := strings.Builder{}
	b.WriteString(c.Type.String() + " " + c.Name)
	if c.Type == ColumnFixedString {
		b.WriteString(" " + strconv.Itoa(c.Strlen))
	}
	return b.String()
}

func NewSchemaFromBytes(buf []byte) *Schema {
	r := bytes.NewReader(buf)
	rd := bufio.NewReader(r)

	var cols []*Column
	for {
		line, _, err := rd.ReadLine()
		if err == io.EOF {
			break
		}
		c := NewColumnFromBytes(line)
		cols = append(cols, c)
	}
	return &Schema{
		Columns: cols,
	}
}

func NewColumnFromBytes(buf []byte) *Column {
	line := strings.Split(string(buf), " ")
	c := &Column{}
	c.Type = WhichColumnType(line[0])
	c.Name = line[1]
	if c.Type == ColumnFixedString {
		c.Strlen, _ = strconv.Atoi(line[2])
	}
	return c
}

type Row struct {
	Schema *Schema
	Values []interface{}
}

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
