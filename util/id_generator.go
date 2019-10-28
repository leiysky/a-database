package util

type IDGenerator interface {
	Gen() Int64
}

type idGen struct {
	current Int64
}

func (g *idGen) Gen() Int64 {
	g.current++
	return g.current
}

func NewIDGenerator() IDGenerator {
	return &idGen{}
}
