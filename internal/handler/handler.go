package handler

import (
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// Handler function type
type Handler func(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error)

// GetHandlers returns the map of command handlers
func GetHandlers() map[string]Handler {
	return map[string]Handler{
		"PING":      ping,
		"ECHO":      echo,
		"SET":       set,
		"GET":       get,
		"RPUSH":     rpush,
		"LRANGE":    lrange,
		"LPUSH":     lpush,
		"BLPOP":     blpop,
		"LLEN":      llen,
		"LPOP":      lpop,
		"TYPE":      typeCmd,
		"XADD":      xadd,
		"XRANGE":    xrange,
		"XREAD":     xread,
		"CONFIG":    config,
		"KEYS":      keys,
		"SUBSCRIBE": subscribe,
		"PUBLISH":   publish,
		"UNSUBSCRIBE": unsubscribe,
		"ZADD":        zadd,
		"ZRANGE":      zrange,
		"ZCARD":       zcard,
		"ZREM":        zrem,
	}
}
