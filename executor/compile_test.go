package executor

import (
	"testing"

	"github.com/leiysky/a-database/parser"
)

func TestCompileSelect(t *testing.T) {
	p := parser.New()
	stmt := p.Parse(`select a, * from b where 1 = 1.1 limit 1`)
	Compile(stmt)
}

func TestCompileInsert(t *testing.T) {
	p := parser.New()
	stmt := p.Parse(`insert into a(c1, c2) values (1, 2)`)
	Compile(stmt)
}
