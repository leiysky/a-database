package util

import (
	"bytes"
	"encoding/binary"
	"time"
)

func ReadRow(row []byte, schema *Schema) *Row {
	columns := make([]interface{}, len(schema.Columns))
	var offset int
	for i, c := range schema.Columns {
		columns[i], offset = readOneColumn(row, offset, c)
	}
	return &Row{
		Schema: schema,
		Values: columns,
	}
}

func readOneColumn(row []byte, offset int, column *Column) (interface{}, int) {
	switch column.Type {
	case ColumnInt32:
		slice := row[offset : offset+4]
		v, _ := binary.ReadVarint(bytes.NewReader(slice))
		return Int32(v), offset + 4
	case ColumnInt64:
		slice := row[offset : offset+8]
		v, _ := binary.ReadVarint(bytes.NewReader(slice))
		return Int64(v), offset + 8
	case ColumnUInt32:
		slice := row[offset : offset+4]
		v, _ := binary.ReadUvarint(bytes.NewReader(slice))
		return UInt32(v), offset + 4
	case ColumnUInt64:
		slice := row[offset : offset+8]
		v, _ := binary.ReadUvarint(bytes.NewReader(slice))
		return UInt64(v), offset + 8
	case ColumnFixedString:
		slice := row[offset : offset+column.Strlen]
		return FixedString(slice), offset + column.Strlen
	case ColumnDate:
		slice := row[offset : offset+8]
		v, _ := binary.ReadVarint(bytes.NewReader(slice))
		return Date(time.Unix(v, 0)), offset + 8
	default:
		return nil, 0
	}
}

func NewRawBuilder() *RawBuilder {
	return &RawBuilder{
		buff: bytes.NewBufferString(""),
	}
}

type RawBuilder struct {
	buff *bytes.Buffer
}

func (b *RawBuilder) Reset() {
	b.buff = bytes.NewBufferString("")
}

func (b *RawBuilder) AppendInt64(v Int64) {
	buf := make([]byte, 8)
	binary.PutVarint(buf, int64(v))
	b.buff.Write(buf)
}

func (b *RawBuilder) AppendInt32(v Int32) {
	buf := make([]byte, 4)
	binary.PutVarint(buf, int64(v))
	b.buff.Write(buf)
}

func (b *RawBuilder) AppendUInt64(v UInt64) {
	buf := make([]byte, 8)
	binary.PutUvarint(buf, uint64(v))
	b.buff.Write(buf)
}

func (b *RawBuilder) AppendUInt32(v UInt32) {
	buf := make([]byte, 4)
	binary.PutUvarint(buf, uint64(v))
	b.buff.Write(buf)
}

func (b *RawBuilder) AppendFixedString(v FixedString) {
	b.buff.Write([]byte(v))
}

func (b *RawBuilder) AppendDate(v Date) {
	buf := make([]byte, 8)
	binary.PutVarint(buf, v.Timestamp())
	b.buff.Write(buf)
}

// Spawn will return a slice of buffer held by RawBuilder.
// The result can be correctly read only before RawBuilder reset.
func (b *RawBuilder) Spawn() []byte {
	return b.buff.Bytes()
}
