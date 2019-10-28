package server

import (
	"github.com/gin-gonic/gin"
)

func ping(ctx *gin.Context) {
	ctx.JSON(200, gin.H{
		"msg": "pong",
	})
}
