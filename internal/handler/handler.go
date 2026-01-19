package handler

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// Handler function type
type Handler func(args []string, kv *store.Kv) (resp.Value, error)

// GetHandlers returns the map of command handlers
func GetHandlers() map[string]Handler {
	return map[string]Handler{
		"PING":   ping,
		"ECHO":   echo,
		"SET":    set,
		"GET":    get,
		"RPUSH":  rpush,
		"LRANGE": lrange,
		"LPUSH":  lpush,
		"BLPOP":  blpop,
		"LLEN":   llen,
		"LPOP":   lpop,
		"TYPE":   typeCmd,
		"XADD":   xadd,
	}
}

func ping(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) == 0 {
		return resp.SimpleString("PONG"), nil
	}
	//in case of argument, return the first argument
	return resp.BulkString(args[0]), nil
}

func echo(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR ECHO requires exactly one argument")
	}
	return resp.BulkString(args[0]), nil
}

func get(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR GET requires exactly one argument")
	}
	val, ok := kv.Get(args[0])
	if !ok {
		return nil, nil // Key not found
	}
	return resp.BulkString(val), nil
}

// parse set
func set(args []string, kv *store.Kv) (resp.Value, error) {
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

func rpush(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR RPUSH requires at least two arguments")
	}
	key := args[0]
	values := args[1:]

	pushedLen := kv.RPush(key, values...)

	return resp.Integer(pushedLen), nil
}

func lrange(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) != 3 {
		return nil, errors.New("ERR LRANGE requires exactly three arguments")
	}
	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("invalid start index")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, errors.New("invalid stop index")
	}
	list, err := kv.LRange(key, start, stop)
	if err != nil {
		return nil, err
	}
	//convert []string to Array of BulkString
	respArray := make(resp.Array, len(list))
	for i, v := range list {
		respArray[i] = resp.BulkString(v)
	}
	return respArray, nil
}

func lpush(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR LPUSH requires at least two arguments")
	}
	key := args[0]
	values := args[1:]
	// Redis LPUSH inserts values from left to right so the last argument
	// ends up at the head. `Kv.LPush` preserves input order, so reverse
	// the values here to match the network semantics.
	rev := make([]string, len(values))
	for i := range values {
		rev[i] = values[len(values)-1-i]
	}
	pushedLen := kv.LPush(key, rev...)
	return resp.Integer(pushedLen), nil
}

func blpop(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) != 2 {
		return nil, errors.New("ERR BLPOP requires exactly two arguments: key and timeout")
	}
	key := args[0]
	timeoutSec, err := strconv.ParseFloat(args[1], 64)
	if err != nil || timeoutSec < 0 {
		return nil, errors.New("ERR invalid timeout")
	}

	// Try immediate pop or register waiter
	val, ch, ok := kv.BlockingPop(key)
	if ok {
		// Immediate success
		response := resp.Array{resp.BulkString(key), resp.BulkString(val)}
		return response, nil
	}

	// Wait for value or timeout. timeoutSec == 0 means block indefinitely.
	if timeoutSec == 0 {
		val := <-ch
		response := resp.Array{resp.BulkString(key), resp.BulkString(val)}
		return response, nil
	}

	timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
	select {
	// Got value not a timeout
	case val := <-ch:
		if !timer.Stop() {
			<-timer.C // drain the timer channel if needed
		}
		response := resp.Array{resp.BulkString(key), resp.BulkString(val)}
		return response, nil
	// Timeout case
	case <-timer.C:
		// timeout: remove waiter
		kv.RemoveWaiter(key, ch)
		return resp.NullArray{}, nil
	}
}

func llen(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR LLEN requires exactly one argument")
	}
	key := args[0]
	length := kv.LLen(key)
	return resp.Integer(length), nil
}

func lpop(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, errors.New("ERR LPOP requires one or two arguments")
	}
	key := args[0]
	n := 1
	if len(args) == 2 {
		var err error
		n, err = strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("invalid count argument")
		}
	}
	vals, err := kv.LPop(key, n)
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}
	if len(vals) == 1 {
		return resp.BulkString(vals[0]), nil
	}
	respArray := make(resp.Array, len(vals))
	for i, v := range vals {
		respArray[i] = resp.BulkString(v)
	}
	return respArray, nil
}

func typeCmd(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("TYPE requires exactly one argument")
	}
	key := args[0]
	res := kv.Type(key)
	return resp.SimpleString(res), nil
}
