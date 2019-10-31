package context

import (
	"github.com/leiysky/a-database/storage"
	"github.com/leiysky/a-database/util"
)

type Context interface {
	Schemas() map[string]*util.Schema
	Store() storage.Storage
}

type context struct {
	schemas map[string]*util.Schema
	store   storage.Storage
}

func (c *context) Schemas() map[string]*util.Schema {
	return c.schemas
}

func (c *context) Store() storage.Storage {
	return c.store
}

func NewContext(schemas map[string]*util.Schema, store storage.Storage) Context {
	return &context{
		schemas: schemas,
		store:   store,
	}
}
