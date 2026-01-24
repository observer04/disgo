package handler

import (
	"errors"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// typeCmd: returns the type of the value stored at key
func typeCmd(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("TYPE requires exactly one argument")
	}
	key := args[0]
	res := kv.Type(key)
	return resp.SimpleString(res), nil
}

// config: gets or sets configuration parameters; currently only supports GET subcommand
func config(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
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
func keys(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
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

// auth: authenticate the client
func auth(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, errors.New("ERR wrong number of arguments for 'auth' command")
	}

	username := "default"
	password := ""

	if len(args) == 2 {
		username = args[0]
		password = args[1]
	} else {
		password = args[0]
	}

	// Use ACL Engine
	user, ok := state.Config.AclEngine.Authenticate(username, password)
	if !ok {
		return nil, errors.New("WRONGPASS invalid username-password pair or user is disabled.") 
	}

	state.Authenticated = true
	state.User = user
	return resp.SimpleString("OK"), nil
}
