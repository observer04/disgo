package handler

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func xadd(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) < 4 || (len(args)-2)%2 != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'xadd' command")
	}

	key := args[0]
	id := args[1]
	values := args[2:]

	newID, err := kv.XAdd(key, id, values)
	if err != nil {
		return nil, err
	}

	return resp.BulkString(newID), nil
}

func xrange(args []string, kv *store.Kv) (resp.Value, error) {
	if len(args) != 3 {
		return nil, errors.New("ERR wrong number of arguments for 'xrange' command")
	}

	key := args[0]
	start := args[1]
	end := args[2]

	entries, err := kv.XRange(key, start, end) //stream entries like: [{ID:1526985058136-0, Values:[key1 val1 key2 val2]} ...]
	if err != nil {
		return nil, err
	}

	// Format response: [[ID, [Key, Val, ...]], ...]
	respEntries := make(resp.Array, len(entries))
	for i, entry := range entries {
		// Entry ID
		id := resp.BulkString(entry.ID)

		// Entry Values (Flat list to Array)
		values := make(resp.Array, len(entry.Values))
		for j, v := range entry.Values {
			values[j] = resp.BulkString(v)
		}

		// Single Entry Row: [ID, [Values]]
		respEntries[i] = resp.Array{id, values}
	}

	return respEntries, nil
}

func xread(args []string, kv *store.Kv) (resp.Value, error) {
	var blockTime int64 = -1 // -1 means no block
	streamsArgIdx := -1      // index of "STREAMS" argument

	// check if we have BLOCK and STREAMS and parse BLOCK time if present
	for i := 0; i < len(args); i++ {
		arg := strings.ToUpper(args[i])
		if arg == "BLOCK" {
			if i+1 >= len(args) {
				return nil, errors.New("ERR syntax error")
			}
			var err error
			blockTime, err = strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, errors.New("ERR value is not an integer or out of range")
			}
			i++
		} else if arg == "STREAMS" {
			streamsArgIdx = i + 1
			break
		}
	}
	//if STREAMS not found or no args after
	if streamsArgIdx == -1 || streamsArgIdx >= len(args) {
		return nil, errors.New("ERR syntax error")
	}

	remaining := args[streamsArgIdx:]
	if len(remaining)%2 != 0 {
		return nil, errors.New("ERR syntax error")
	}
	//example: remaining = [stream1 stream2 id1 id2]
	count := len(remaining) / 2
	keys := remaining[:count]
	ids := remaining[count:]

	streamMap := make(map[string]string)
	for i, key := range keys {
		streamMap[key] = ids[i]
	}

	// First pass: Resolve "$" to last ID
	for i, id := range ids {
		if id == "$" {
			lastID, err := kv.GetLastID(keys[i])
			if err != nil {
				return nil, err
			}
			streamMap[keys[i]] = lastID
		}
	}

	// If blocking, enter loop
	if blockTime >= 0 {
		var timeoutChan <-chan time.Time
		if blockTime > 0 {
			// send after block time elapses
			timeoutChan = time.After(time.Duration(blockTime) * time.Millisecond)
		}

		// Create a waiter for all keys
		waiter := kv.NewStreamWaiter(keys)
		defer kv.RemoveStreamWaiter(keys, waiter)

		// Check immediately first (unless it's a pure $ block start, but simple check handles it)
		res, err := kv.XRead(streamMap)
		if err != nil {
			return nil, err
		}
		if len(res) > 0 {
			return formatXReadResponse(keys, res), nil
		}

		// Wait loop
		for {
			select {
			case <-waiter:
				// Something arrived, check again
				res, err := kv.XRead(streamMap)
				if err != nil {
					return nil, err
				}
				if len(res) > 0 {
					return formatXReadResponse(keys, res), nil
				}
			case <-timeoutChan:
				// Timeout
				return resp.NullArray{}, nil
			}
			// If blockTime == 0 (infinite), we just loop back on channel receive
		}
	}

	// Non-blocking read
	res, err := kv.XRead(streamMap)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return resp.NullArray{}, nil
	}

	return formatXReadResponse(keys, res), nil
}

func formatXReadResponse(keys []string, data map[string][]store.StreamEntry) resp.Value {
	// Response is Array of Arrays: [[Key, [[ID, [Key, Val...]], ...]], ...]
	// Order must match the order of keys in arguments.
	var topLevel resp.Array

	for _, key := range keys {
		entries, ok := data[key]
		if !ok || len(entries) == 0 {
			continue
		}

		streamArr := resp.Array{}
		streamArr = append(streamArr, resp.BulkString(key))

		entriesArr := resp.Array{}
		for _, e := range entries {
			id := resp.BulkString(e.ID)
			vals := resp.Array{}
			for _, v := range e.Values {
				vals = append(vals, resp.BulkString(v))
			}
			entriesArr = append(entriesArr, resp.Array{id, vals})
		}
		streamArr = append(streamArr, entriesArr)
		topLevel = append(topLevel, streamArr)
	}

	return topLevel
}
