package util

import (
	"testing"
	"time"

	"github.com/leiysky/go-utils/assert"
)

func TestEncoding(t *testing.T) {
	assert := assert.New(t)
	now := Date(time.Now())
	b := &RawBuilder{}
	b.AppendInt64(123)
	b.AppendInt32(-321)
	b.AppendUInt32(1234)
	b.AppendUInt64(4321)
	b.AppendFixedString("1234")
	b.AppendDate(now)

	buff := b.Spawn()

	schema := &Schema{
		Columns: []*Column{
			&Column{
				Type: ColumnInt64,
			},
			&Column{
				Type: ColumnInt32,
			},
			&Column{
				Type: ColumnUInt32,
			},
			&Column{
				Type: ColumnUInt64,
			},
			&Column{
				Type:   ColumnFixedString,
				Strlen: 4,
			},
			&Column{
				Type: ColumnDate,
			},
		},
	}

	row := ReadRow(buff, schema)

	assert.Equal(row.Values[0], Int64(123))
	assert.Equal(row.Values[1], Int32(-321))
	assert.Equal(row.Values[2], UInt32(1234))
	assert.Equal(row.Values[3], UInt64(4321))
	assert.Equal(row.Values[4], FixedString("1234"))
	assert.Equal(row.Values[5].(Date).Timestamp(), now.Timestamp())
}
