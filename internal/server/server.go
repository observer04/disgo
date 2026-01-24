package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/codecrafters-io/redis-starter-go/internal/acl"
	"github.com/codecrafters-io/redis-starter-go/internal/handler"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func Start(dir, dbfilename, requirePass string) {
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

	// Initialize ACL Engine
	aclEngine := acl.NewEngine(requirePass)

	// Get handlers
	commandHandlers := handler.GetHandlers()

	// Configuration
	config := &handler.Config{
		RequirePass: requirePass,
		AclEngine:   aclEngine,
	}

	//Accept connections in a loop
	for {
		con, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err.Error())
			continue
		}

		//Handle Client connections
		go HandleClient(con, kvStore, commandHandlers, config)
	}
}

func HandleClient(con net.Conn, kv *store.Kv, handlers map[string]handler.Handler, config *handler.Config) {
	// We need to support concurrent reading (commands) and writing (pub/sub messages)
	// Create a channel for messages destined for this client
	msgCh := make(chan interface{}, 100) // buffer to avoid blocking
	defer close(msgCh)
	
	defaultUser, _ := config.AclEngine.GetUser("default")
	authenticated := false
	if defaultUser.Password == "" {
		authenticated = true
	}

	connState := &handler.ConnectionState{
		Authenticated: authenticated,
		User:          defaultUser,
		Config:        config,
	}

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

		response, err := handler.Dispatch(args[0], args[1:], kv, msgCh, handlers, connState)
		if err != nil {
			// In Dispatcher, we might return resp.Error as error or as value?
			// The current signature is (resp.Value, error).
			// If error is returned, it might be a Go error or a resp.Error (if we cast it).
			// Let's check Dispatcher implementation.
			// It returns `resp.Error(...)` which satisfies `error` interface? No.
			// `resp.Error` is `string`. It doesn't satisfy `error`.
			// Wait, `resp.Error` is `type Error string`.
			// In `dispatcher.go`: `return nil, resp.Error(...)`.
			// `resp.Error` does NOT implement `error`.
			// So `handler.Dispatch` signature `(resp.Value, error)` is wrong if it returns `resp.Error` as 2nd arg.
			// `resp.Error` is a `Value`.
			// Dispatcher should return `(resp.Value, error)` where `error` is a system error,
			// and `resp.Value` can be `resp.Error`.
			// OR Dispatcher returns `(resp.Value, error)` and `resp.Error` is returned as `resp.Value`.

			// Let's correct `dispatcher.go` first.
			// Re-reading `dispatcher.go` logic I wrote...
			// `return nil, resp.Error(...)` -> This implies `resp.Error` is passed as `error`.
			// Go compiler will fail if `resp.Error` doesn't implement `error`.
			// `type Error string`. It does NOT implement `Error() string`.
			// So I need to fix Dispatcher.
		}
		// Assuming Dispatcher returns `(resp.Value, error)` where error is real go error.
		// If command failed with Redis error, it should be in `resp.Value`.
		
		// Let's fix Dispatcher logic in next step.
		// For now, let's write the `HandleClient` assuming Dispatcher works as:
		// Returns (response, nil) for success (including Redis errors).
		// Returns (nil, err) for system errors.
		
		if err != nil {
			log.Printf("Dispatch error: %v", err)
			continue
		}

		if response != nil {
			msgCh <- response
		}
	}
}
