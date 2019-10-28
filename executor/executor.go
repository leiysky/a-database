package executor

type Executor interface {
	Next() bool
	Open()
	Close()
}