package util

import (
	"fmt"
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

func TestPrettify(t *testing.T) {
	schema := &Schema{
		Columns: []*Column{
			&Column{
				Name: "c1",
				Type: ColumnInt32,
			},
			&Column{
				Name:   "c2",
				Type:   ColumnFixedString,
				Strlen: 5,
			},
		},
	}

	rows := []*Row{
		&Row{
			Schema: schema,
			Values: []interface{}{
				123,
				"hello",
			},
		},
		&Row{
			Schema: schema,
			Values: []interface{}{
				321,
				"hello1",
			},
		},
		&Row{
			Schema: schema,
			Values: []interface{}{
				456,
				"hello2",
			},
		},
	}

	fmt.Println(Prettify(rows))
}
