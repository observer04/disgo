package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		log.Fatal("Failed to bind to port 6379", err)
	}

	con, err := l.Accept()
	if err != nil {
		log.Fatal("Error accepting connection: ", err.Error())
	}
	con.Write([]byte("+PONG\r\n"))
}
