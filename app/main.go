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
)

var _ = os.Exit

type RespValue interface{}

type SimpleString string
type BulkString string

// Kv is a simple in-memory key-value store with mutex for concurrency safety.
type Kv struct {
	mu   sync.Mutex
	data map[string]string
}

// constructor function for Kv
func NewKv() *Kv {
	return &Kv{data: make(map[string]string)}
}

// Set stores the key-value pair in the Kv store.
func (k *Kv) Set(key, value string) {
	k.mu.Lock()
	k.data[key] = value
	k.mu.Unlock()
}

func (k *Kv) Get(key string) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	val, ok := k.data[key]
	return val, ok
}

// Handlers
type Handler func(args []string, kv *Kv) (RespValue, error)

// Map of command names to their handlers
var handlers = map[string]Handler{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
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

func set(args []string, kv *Kv) (RespValue, error) {
	if len(args) != 2 {
		return nil, errors.New("SET requires exactly two arguments")
	}
	kv.Set(args[0], args[1])
	return SimpleString("OK"), nil
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
