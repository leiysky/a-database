package parser

import (
	"github.com/xwb1989/sqlparser"
)

type Parser struct {
}

func New() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(sql string) sqlparser.Statement {
	stmt, _ := sqlparser.Parse(sql)
	return stmt
}
