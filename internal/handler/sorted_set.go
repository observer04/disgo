package handler

import (
	"errors"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func zadd(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) < 3 || len(args)%2 == 0 { // key score member [score member]... -> odd number of args (key + pairs)
		return nil, errors.New("ERR wrong number of arguments for 'zadd' command")
	}

	key := args[0]
	members := make([]store.SortedSetNode, 0, (len(args)-1)/2)

	for i := 1; i < len(args); i += 2 {
		scoreStr := args[i]
		member := args[i+1]
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return nil, errors.New("ERR value is not a valid float")
		}
		members = append(members, store.SortedSetNode{Member: member, Score: score})
	}

	added := kv.ZAdd(key, members...)
	return resp.Integer(int64(added)), nil
}

func zrange(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) < 3 {
		return nil, errors.New("ERR wrong number of arguments for 'zrange' command")
	}
	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}

	withScores := false
	if len(args) > 3 {
		if strings.ToUpper(args[3]) == "WITHSCORES" {
			withScores = true
		} else {
			return nil, errors.New("ERR syntax error")
		}
	}

	items := kv.ZRange(key, start, stop)
	
	respItems := make([]resp.Value, 0, len(items))
	for _, item := range items {
		respItems = append(respItems, resp.BulkString(item.Member))
		if withScores {
			scoreStr := strconv.FormatFloat(item.Score, 'g', -1, 64)
			respItems = append(respItems, resp.BulkString(scoreStr))
		}
	}
	return resp.Array(respItems), nil
}

func zcard(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'zcard' command")
	}
	key := args[0]
	count := kv.ZCard(key)
	return resp.Integer(int64(count)), nil
}

func zrem(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'zrem' command")
	}
	key := args[0]
	members := args[1:]
	removed := kv.ZRem(key, members...)
	return resp.Integer(int64(removed)), nil
}