package parser

type Node interface {
	Children() []Node
	Name() string
}

type SelectStmt struct {
	ResultFields [][]byte
}