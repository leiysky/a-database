package executor

import (
	"testing"

	"github.com/leiysky/a-database/parser"
)

func TestCompile(t *testing.T) {
	p := parser.New()
	stmt := p.Parse(`select a from b`)
	Compile(stmt)
}
