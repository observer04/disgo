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
