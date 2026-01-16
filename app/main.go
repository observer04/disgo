package main

import (
	"os"
	"github.com/codecrafters-io/redis-starter-go/internal/server"
)

var _ = os.Exit

func main() {
	server.Start()
}