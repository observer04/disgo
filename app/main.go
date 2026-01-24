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

	replHost := ""
	replPort := 0
	if *replicaof != "" {
		parts := strings.Fields(*replicaof)
		if len(parts) > 0 {
			replHost = parts[0]
		}
		if len(parts) > 1 {
			if p, err := strconv.Atoi(parts[1]); err == nil {
				replPort = p
			}
		}
		if replPort == 0 && strings.Contains(replHost, ":") {
			hostParts := strings.SplitN(replHost, ":", 2)
			replHost = hostParts[0]
			if p, err := strconv.Atoi(hostParts[1]); err == nil {
				replPort = p
			}
		}
	}

	server.Start(*dir, *dbfilename, *requirePass, replHost, replPort, *port)
}
