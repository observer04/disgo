package handler

import (
	"errors"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// rpush: append one or more values to the end of a list
func rpush(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR RPUSH requires at least two arguments")
	}
	key := args[0]
	values := args[1:]

	pushedLen := kv.RPush(key, values...)

	return resp.Integer(pushedLen), nil
}

// lrange: get a range of elements from a list; start and stop are inclusive
func lrange(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
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

// lpush: prepend one or more values to the start of a list; values are inserted from left to right;
func lpush(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
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

// blpop: remove and get the first element in a list, or block until one is available; timeout in seconds; 0 means block indefinitely
func blpop(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
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

// llen: get the length of a list
func llen(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR LLEN requires exactly one argument")
	}
	key := args[0]
	length := kv.LLen(key)
	return resp.Integer(length), nil
}

// lpop: remove and get the first element in a list; with optional count argument
func lpop(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
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
		if len(args) == 2 {
			return resp.NullArray{}, nil
		}
		return resp.NullBulkString{}, nil
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
