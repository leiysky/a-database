package server

import (
	"github.com/gin-gonic/gin"
	"github.com/leiysky/a-database/util"
)

func (s *Server) ping(ctx *gin.Context) {
	ctx.JSON(200, gin.H{
		"msg": "pong",
	})
}

func (s *Server) query(ctx *gin.Context) {
	type Req struct {
		Query string `json:"query"`
	}
	req := &Req{}
	err := ctx.BindJSON(req)
	if err != nil {
		ctx.JSON(400, gin.H{
			"msg": err.Error(),
		})
	}
	results := s.db.ExecuteQuery(req.Query)

	ctx.String(200, util.Prettify(results))
}

func (s *Server) route() {
	s.http.GET("/ping", s.ping)
	s.http.POST("/query", s.query)
}
