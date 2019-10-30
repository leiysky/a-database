package context

import (
	"github.com/leiysky/a-database/storage"
	"github.com/leiysky/a-database/util"
)

type Context struct {
	Schemas map[string]*util.Schema
	Store   storage.Storage
}
