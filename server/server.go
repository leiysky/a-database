package server

import (
	"github.com/gin-gonic/gin"
	"github.com/leiysky/a-database/util"
)

type Server struct {
	id   util.IDGenerator
	http *gin.Engine
	cfg  *Config
}

func NewServer(config *Config) *Server {
	return &Server{
		id:   util.NewIDGenerator(0),
		http: gin.New(),
		cfg:  config,
	}
}

func (s *Server) Run() {
	err := s.http.Run(":" + s.cfg.HttpPort)
	if err != nil {
		panic(err)
	}
}
