package main

import (
	"flag"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/server"
)

var _ = os.Exit

func main() {
	dir := flag.String("dir", "", "The directory where the RDB file is stored")
	dbfilename := flag.String("dbfilename", "", "The name of the RDB file")
	requirePass := flag.String("requirepass", "", "The password required to authenticate")
	flag.Parse()

	server.Start(*dir, *dbfilename, *requirePass)
}
