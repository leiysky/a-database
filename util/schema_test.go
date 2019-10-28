package util

import (
	"testing"

	"github.com/leiysky/go-utils/assert"
)

func TestSchema(t *testing.T) {
	assert := assert.New(t)
	s := &Schema{
		Columns: []*Column{
			&Column{
				Type: ColumnInt64,
				Name: "i64",
			},
			&Column{
				Type: ColumnUInt64,
				Name: "u64",
			},
			&Column{
				Type:   ColumnFixedString,
				Name:   "str",
				Strlen: 3,
			},
		},
	}

	tmp := s.String()
	buff := []byte(tmp)
	newSchema := NewSchemaFromBytes(buff)
	assert.Equal(tmp, newSchema.String())
}
