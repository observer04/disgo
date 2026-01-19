package handler

import (
	"errors"

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
