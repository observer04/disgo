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
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func Start(dir, dbfilename string) {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Fatal("Failed to bind to port 6379", err)
	}

	defer l.Close()
	fmt.Println("Server listening on 6379")

	// Initialize key-value store
	kvStore := store.NewKv(dir, dbfilename)

	// Load RDB file if it exists
	if err := rdb.Load(dir, dbfilename, kvStore); err != nil {
		log.Printf("Failed to load RDB file: %v", err)
	}

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
	// We need to support concurrent reading (commands) and writing (pub/sub messages)
	// Create a channel for messages destined for this client
	msgCh := make(chan interface{}, 100) // buffer to avoid blocking
	defer close(msgCh)
	// We also need to clean up subscriptions on exit
	// But `kv.Unsubscribe` requires the channel.
	// The store doesn't support "UnsubscribeAll".
	// We can track subscribed channels here.
	// But `subscribe` handler is where subscription happens.
	// We don't have visibility here.
	// Ideally `kv` should support `UnsubscribeAll(ch)`.
	// For now, if we don't unsubscribe, the map grows.
	// Let's implement `UnsubscribeAll` in store.

	// Write Loop
	go func() {
		defer con.Close()
		w := bufio.NewWriter(con)
		for msg := range msgCh {
			if err := resp.Write(w, msg); err != nil {
				return // connection likely closed
			}
			w.Flush()
		}
	}()

	// Read Loop
	r := bufio.NewReader(con)
	for {
		args, err := resp.Read(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean disconnect
			} else {
				log.Printf("problem reading from connection: %v", err)
			}
			// Unsubscribe from all (Todo: implement efficiently)
			kv.UnsubscribeAll(msgCh)
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		cmdHandler, ok := handlers[cmd]
		if !ok {
			msgCh <- resp.Error(fmt.Sprintf("ERR unknown command '%s'", args[0]))
			continue
		}

		response, err := cmdHandler(args[1:], kv, msgCh)
		if err != nil {
			msgCh <- resp.Error(err.Error())
			continue
		}

		if response != nil {
			msgCh <- response
		}
	}
}
