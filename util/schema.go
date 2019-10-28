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

type Int32 int

func (v Int32) TypeName() string {
	return "Int32"
}

type Int64 int64

func (v Int64) TypeName() string {
	return "Int64"
}

type UInt32 uint

func (v UInt32) TypeName() string {
	return "UInt32"
}

type UInt64 uint64

func (v UInt64) TypeName() string {
	return "UInt64"
}

type FixedString []byte

func (v FixedString) TypeName() string {
	return "FixedString"
}

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
	Columns []*Column
}

func (s *Schema) String() string {
	b := strings.Builder{}
	for _, c := range s.Columns {
		b.WriteString(c.String() + "\n")
	}
	return b.String()
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
