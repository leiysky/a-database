package server

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/leiysky/a-database/context"
	"github.com/leiysky/a-database/db"
	"github.com/leiysky/a-database/storage"
	"github.com/leiysky/a-database/util"
)

type Server struct {
	id   util.IDGenerator
	http *gin.Engine
	cfg  *Config
	db   *db.DB
}

func NewServer(config *Config) *Server {
	return &Server{
		id:   util.NewIDGenerator(0),
		http: gin.New(),
		cfg:  config,
	}
}

func (s *Server) Run() {
	s.bootstrap()
	err := s.http.Run(":" + s.cfg.HttpPort)
	if err != nil {
		panic(err)
	}
}

// initialize environment
func (s *Server) bootstrap() {
	storeCfg := &storage.Config{
		Path: path.Join(s.cfg.DataPath, "db"),
	}
	store := storage.NewKVStorage(storeCfg)

	schemaPath := path.Join(s.cfg.DataPath, "schema")
	schemas := make(map[string]*util.Schema)
	filepath.Walk(schemaPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		buff, _ := ioutil.ReadFile(path)
		schema := util.NewSchemaFromBytes(buff)
		schema.TableName = filepath.Base(path)
		schemas[filepath.Base(path)] = schema
		return nil
	})

	ctx := context.NewContext(schemas, store)
	s.db = db.NewDB(ctx)

	s.route()
}
