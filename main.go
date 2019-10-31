package main

import (
	"path/filepath"

	"github.com/leiysky/a-database/server"
)

func run() {
	path, err := filepath.Abs("examples")
	if err != nil {
		panic(err)
	}
	cfg := &server.Config{
		HttpPort: "3399",
		DataPath: path,
	}
	server := server.NewServer(cfg)
	server.Run()
}

func main() {
	run()
}
