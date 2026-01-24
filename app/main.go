package main

import (
	"flag"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/server"
)

var _ = os.Exit

func main() {
	dir := flag.String("dir", "", "The directory where the RDB file is stored")
	dbfilename := flag.String("dbfilename", "", "The name of the RDB file")
	requirePass := flag.String("requirepass", "", "The password required to authenticate")
	replicaof := flag.String("replicaof", "", "The master host for replication")
	port := flag.Int("port", 6379, "The port to listen on")
	flag.Parse()

	replHost := *replicaof
	replPort := 0
	if replHost != "" {
		if strings.Contains(replHost, ":") {
			parts := strings.SplitN(replHost, ":", 2)
			replHost = parts[0]
			if p, err := strconv.Atoi(parts[1]); err == nil {
				replPort = p
			}
		} else if len(flag.Args()) > 0 {
			if p, err := strconv.Atoi(flag.Args()[0]); err == nil {
				replPort = p
			}
		}
	}

	server.Start(*dir, *dbfilename, *requirePass, replHost, replPort, *port)
}
