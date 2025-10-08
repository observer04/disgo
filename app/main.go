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

	defer l.Close()
	fmt.Println("Server listening on 6379")

	for {
		con, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err.Error())
			continue
		}

		//Handle Client connections
		go handleClient(con)
	}

}

func handleClient(con net.Conn) {
	defer con.Close()
	buf := make([]byte, 128)
	n, err := con.Read(buf)
	if err != nil {
		log.Fatal("problem reading from buffer")
	}
	log.Printf("Received Data: ", buf[:n])
	msg := []byte("+PONG\r\n")
	_, err = con.Write(msg)
	if err != nil {
		log.Fatal("problem writing to buffer")
	}
}
