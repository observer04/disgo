package handler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func geoadd(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) < 4 || (len(args)-1)%3 != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'geoadd' command")
	}

	key := args[0]
	// count = (len - 1) / 3
	count := (len(args) - 1) / 3
	members := make([]store.SortedSetNode, 0, count)

	idx := 1
	for i := 0; i < count; i++ {
		longStr := args[idx]
		latStr := args[idx+1]
		member := args[idx+2]
		idx += 3

		long, err := strconv.ParseFloat(longStr, 64)
		if err != nil {
			return nil, errors.New("ERR value is not a valid float")
		}
		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			return nil, errors.New("ERR value is not a valid float")
		}

		// Validate ranges
		if lat < -85.05112878 || lat > 85.05112878 || long < -180 || long > 180 {
			return nil, fmt.Errorf("ERR invalid longitude,latitude pair %f,%f", long, lat)
		}

		hash := store.Encode(lat, long)
		// Store as float64 score
		members = append(members, store.SortedSetNode{Member: member, Score: float64(hash)})
	}

	added := kv.ZAdd(key, members...)
	return resp.Integer(int64(added)), nil
}

func geodist(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) < 3 {
		return nil, errors.New("ERR wrong number of arguments for 'geodist' command")
	}

	key := args[0]
	m1 := args[1]
	m2 := args[2]
	unit := "m"
	if len(args) > 3 {
		unit = args[3]
	}

	score1, ok1 := kv.ZScore(key, m1)
	score2, ok2 := kv.ZScore(key, m2)

	if !ok1 || !ok2 {
		return resp.NullBulkString{}, nil
	}

	lat1, long1 := store.Decode(uint64(score1))
	lat2, long2 := store.Decode(uint64(score2))

	distMeters := store.Distance(lat1, long1, lat2, long2)
	
	var dist float64
	switch strings.ToLower(unit) {
	case "m":
		dist = distMeters
	case "km":
		dist = distMeters / 1000
	case "ft":
		dist = distMeters * 3.28084
	case "mi":
		dist = distMeters * 0.000621371
	default:
		return nil, errors.New("ERR unsupported unit provided. please use m, km, ft, mi")
	}

	return resp.BulkString(strconv.FormatFloat(dist, 'f', 4, 64)), nil
}

func geopos(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'geopos' command")
	}
	key := args[0]
	members := args[1:]
	
	results := make([]resp.Value, len(members))
	for i, m := range members {
		score, ok := kv.ZScore(key, m)
		if !ok {
			results[i] = resp.NullArray{} // or nil? Redis returns nil (null bulk string) inside array for missing elements. Actually array of arrays.
			// If missing, it's a null element in the main array.
			// Let's check docs: "returns an array where each element is a two elements array... or nil"
			// resp.NullArray fits if we treat the inner array as potentially null.
			// Wait, simple resp.NullBulkString? No, it expects an array of 2 items.
			results[i] = resp.NullArray{} 
		} else {
			lat, long := store.Decode(uint64(score))
			results[i] = resp.Array{
				resp.BulkString(strconv.FormatFloat(long, 'f', -1, 64)),
				resp.BulkString(strconv.FormatFloat(lat, 'f', -1, 64)),
			}
		}
	}
	return resp.Array(results), nil
}

func geohash(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'geohash' command")
	}
	key := args[0]
	members := args[1:]
	
	results := make([]resp.Value, len(members))
	for i, m := range members {
		score, ok := kv.ZScore(key, m)
		if !ok {
			results[i] = resp.NullBulkString{}
		} else {
			hashStr := store.ToGeohashString(uint64(score))
			results[i] = resp.BulkString(hashStr)
		}
	}
	return resp.Array(results), nil
}
