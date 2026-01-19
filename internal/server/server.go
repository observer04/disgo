package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/handler"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func Start() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Fatal("Failed to bind to port 6379", err)
	}

	defer l.Close()
	fmt.Println("Server listening on 6379")

	// Initialize key-value store
	kvStore := store.NewKv()

	// Start background expiration loop
	go kvStore.RunExpirationLoop()

	// Get handlers
	commandHandlers := handler.GetHandlers()

	//Accept connections in a loop
	for {
		con, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err.Error())
			continue
		}

		//Handle Client connections
		go HandleClient(con, kvStore, commandHandlers)
	}
}

func HandleClient(con net.Conn, kv *store.Kv, handlers map[string]handler.Handler) {
	defer con.Close()
	r := bufio.NewReader(con)
	w := bufio.NewWriter(con)

	for {
		args, err := resp.Read(r)

		if errors.Is(err, io.EOF) {
			log.Print("EOF reached")
			return
		}
		if err != nil {
			log.Printf("problem reading from connection: %v", err)
			return
		}
		if len(args) == 0 {
			log.Print("empty command received")
			continue
		}

		cmd := strings.ToUpper(args[0])
		cmdHandler, ok := handlers[cmd]
		if !ok {
			errMsg := "-ERR unknown command\r\n"
			w.WriteString(errMsg)
			w.Flush()
			continue
		}

		response, err := cmdHandler(args[1:], kv)
		if err != nil {
			errMsg := fmt.Sprintf("-%s\r\n", err.Error())
			w.WriteString(errMsg)
			w.Flush()
			continue
		}

		if err := resp.Write(w, response); err != nil {
			log.Printf("problem writing response: %v", err)
			return
		}
		if err := w.Flush(); err != nil {
			log.Printf("problem flushing response: %v", err)
			return
		}

		log.Printf("Received Data: %q", args)
	}
}
