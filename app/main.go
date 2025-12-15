package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var _ = os.Exit

type RespValue interface{}

type SimpleString string
type BulkString string
type integer int64

// Kv is a simple in-memory key-value store with mutex for concurrency safety.
type Kv struct {
	mu    sync.Mutex
	data  map[string]string
	exp   map[string]time.Time
	lists map[string][]string
}

// constructor function for Kv
func NewKv() *Kv {
	return &Kv{
		data:  make(map[string]string),
		exp:   make(map[string]time.Time),
		lists: make(map[string][]string),
	}
}

// Set stores the key-value pair in the Kv store with an optional TTL
func (k *Kv) SetWithTTL(key, value string, ttl time.Duration) {
	k.mu.Lock()
	k.data[key] = value
	if ttl > 0 {
		k.exp[key] = time.Now().Add(ttl)
	} else {
		delete(k.exp, key)
	}
	k.mu.Unlock()
}

// without expiration
func (k *Kv) Set(key, value string) {
	k.SetWithTTL(key, value, 0)
}

func (k *Kv) Get(key string) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Check for expiration
	if expTime, ok := k.exp[key]; ok {
		if time.Now().After(expTime) {
			// Key has expired
			delete(k.data, key)
			delete(k.exp, key)
			return "", false
		}
	}
	val, ok := k.data[key]
	return val, ok
}

// list operations:
// RPUSH : append values to the list stored at key
func (k *Kv) RPush(key string, values ...string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.lists[key] = append(k.lists[key], values...)
	return len(k.lists[key])
}

// Handlers
type Handler func(args []string, kv *Kv) (RespValue, error)

// Map of command names to their handlers
var handlers = map[string]Handler{
	"PING":  ping,
	"ECHO":  echo,
	"SET":   set,
	"GET":   get,
	"RPUSH": rpush,
}

// Handlers for redis client commands

func ping(args []string, kv *Kv) (RespValue, error) {
	if len(args) == 0 {
		return SimpleString("PONG"), nil
	}
	//in case of argument, return the first argument
	return BulkString(args[0]), nil
}

func echo(args []string, kv *Kv) (RespValue, error) {
	if len(args) != 1 {
		return nil, errors.New("ECHO requires exactly one argument")
	}
	return BulkString(args[0]), nil
}

func get(args []string, kv *Kv) (RespValue, error) {
	if len(args) != 1 {
		return nil, errors.New("GET requires exactly one argument")
	}
	val, ok := kv.Get(args[0])
	if !ok {
		return nil, nil // Key not found
	}
	return BulkString(val), nil
}

// parse set
func set(args []string, kv *Kv) (RespValue, error) {
	if len(args) < 2 {
		return nil, errors.New("SET requires atleast two arguments")
	}
	key := args[0]
	value := args[1]
	var ttl time.Duration

	// Check for optional PX and EX argument for expiration in milliseconds
	if len(args) > 2 {
		i := 2
		for i < len(args) {
			option := strings.ToUpper(args[i])
			if option == "PX" && i+1 < len(args) {
				ms, err := strconv.Atoi(args[i+1])
				if err != nil {
					return nil, errors.New("invalid PX value")
				}
				ttl = time.Duration(ms) * time.Millisecond
				i += 2
				continue
			}
			if option == "EX" && i+1 < len(args) {
				seconds, err := strconv.Atoi(args[i+1])
				if err != nil {
					return nil, errors.New("invalid EX value")
				}
				ttl = time.Duration(seconds) * time.Second
				i += 2
				continue
			}
			return nil, errors.New("invalid SET option")

		}
	}

	kv.SetWithTTL(key, value, ttl)
	return SimpleString("OK"), nil
}

func rpush(args []string, kv *Kv) (RespValue, error) {
	if len(args) < 2 {
		return nil, errors.New("RPUSH requires at least two arguments")
	}
	key := args[0]
	values := args[1:]
	pushedLen := kv.RPush(key, values...)
	return integer(pushedLen), nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Fatal("Failed to bind to port 6379", err)
	}

	defer l.Close()
	fmt.Println("Server listening on 6379")

	// Initialize key-value store
	kvStore := NewKv()

	// Goroutine to handle expiration of keys
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			kvStore.mu.Lock()
			for k, exp := range kvStore.exp {
				if now.After(exp) {
					delete(kvStore.data, k)
					delete(kvStore.exp, k)
				}
			}
			kvStore.mu.Unlock()
		}
	}()

	//Accept connections in a loop
	for {
		con, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err.Error())
			continue
		}

		//Handle Client connections
		go handleClient(con, kvStore)
	}
}

// validate and read a line ending with CRLF
func readLineCRLF(r *bufio.Reader) (string, error) {
	b, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return "", errors.New("line does not end with CRLF")
	}
	return string(b[:len(b)-2]), nil
}

func readRespArray(r *bufio.Reader) ([]string, error) {
	peek, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	if peek[0] == '*' {
		// Array
		header, err := readLineCRLF(r)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, errors.New("invalid array length")
		}
		args := make([]string, 0, count) // Preallocate slice with capacity, length 0, to avoid nil slice
		for i := 0; i < count; i++ {
			line, err := readLineCRLF(r)
			if err != nil {
				return nil, err
			}
			if line[0] != '$' {
				return nil, errors.New("expected bulk string")
			}
			length, err := strconv.Atoi(line[1:])
			if err != nil {
				return nil, errors.New("invalid bulk string length")
			}
			// if length is -1, it's a null bulk string, args append empty string
			if length < 0 {
				args = append(args, "")
				continue
			}
			buf := make([]byte, length+2) // +2 for CRLF
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			if !bytes.HasSuffix(buf, []byte("\r\n")) {
				return nil, errors.New("bulk string does not end with CRLF")
			}

			args = append(args, string(buf[:length]))

		}
		return args, nil
	}

	//inline command mode without RESP array for nc clients
	line, err := readLineCRLF(r)
	if err != nil {
		return nil, err
	}
	args := strings.Fields(line)
	return args, nil

}

// write RESP value to writer
func writeResp(w *bufio.Writer, val RespValue) error {
	switch v := val.(type) {
	case nil:
		// Null bulk string
		_, err := w.WriteString("$-1\r\n")
		return err
	case SimpleString:
		if _, err := w.WriteString(fmt.Sprintf("+%s\r\n", string(v))); err != nil {
			return err
		}
		return nil

	case BulkString:

		if _, err := w.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)); err != nil {
			return err
		}
		return nil
	case integer:
		if _, err := w.WriteString(fmt.Sprintf(":%d\r\n", v)); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("unsupported RESP type")
	}
}

func handleClient(con net.Conn, kv *Kv) {
	defer con.Close()
	r := bufio.NewReader(con)
	w := bufio.NewWriter(con)

	for {
		// line, err := r.ReadString('\n')
		args, err := readRespArray(r)

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
		handler, ok := handlers[cmd]
		if !ok {
			errMsg := fmt.Sprintf("-Err unknown command\r\n")
			w.WriteString(errMsg)
			w.Flush()
			continue
		}

		resp, err := handler(args[1:], kv)
		if err != nil {
			errMsg := fmt.Sprintf("-Err %s\r\n", err.Error())
			w.WriteString(errMsg)
			w.Flush()
			continue
		}

		if err := writeResp(w, resp); err != nil {
			log.Printf("problem writing response: %v", err)
			return
		}
		if err := w.Flush(); err != nil {
			log.Printf("problem flushing response: %v", err)
			return
		}

		log.Printf("Received Data: %q", args)

		// if _, err := con.Write([]byte("+PONG\r\n")); err != nil {
		// 	log.Print("problem writing to connection")
		// 	break
		// }
	}
}
