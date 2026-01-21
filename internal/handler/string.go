package handler

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func ping(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) == 0 {
		return resp.SimpleString("PONG"), nil
	}
	//in case of argument, return the first argument
	return resp.BulkString(args[0]), nil
}

func echo(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR ECHO requires exactly one argument")
	}
	return resp.BulkString(args[0]), nil
}

func get(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR GET requires exactly one argument")
	}
	val, ok := kv.Get(args[0])
	if !ok {
		return resp.NullBulkString{}, nil // Key not found
	}
	return resp.BulkString(val), nil
}

// parse set
func set(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR SET requires atleast two arguments")
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
	return resp.SimpleString("OK"), nil
}
