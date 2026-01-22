package handler

import (
	"errors"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// typeCmd: returns the type of the value stored at key
func typeCmd(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("TYPE requires exactly one argument")
	}
	key := args[0]
	res := kv.Type(key)
	return resp.SimpleString(res), nil
}

// config: gets or sets configuration parameters; currently only supports GET subcommand
func config(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR CONFIG requires at least two arguments")
	}

	subCmd := strings.ToUpper(args[0])
	if subCmd == "GET" {
		if len(args) != 2 {
			return nil, errors.New("ERR CONFIG GET requires exactly one argument")
		}
		key := args[1]
		val := kv.GetConfig(key)
		// Return array of [key, value]
		return resp.Array{resp.BulkString(key), resp.BulkString(val)}, nil
	}

	return nil, errors.New("ERR unknown CONFIG subcommand")
}

// keys: returns all keys matching the given pattern;
func keys(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR KEYS requires exactly one argument")
	}

	pattern := args[0]
	keys := kv.Keys(pattern) // []string of matching keys

	respKeys := make(resp.Array, len(keys)) //convert []string to Array of BulkString
	for i, k := range keys {
		respKeys[i] = resp.BulkString(k)
	}

	return respKeys, nil
}
