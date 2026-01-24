package handler

import (
	"github.com/codecrafters-io/redis-starter-go/internal/acl"
	"github.com/codecrafters-io/redis-starter-go/internal/replication"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// ConnectionState holds the state of a client connection
type ConnectionState struct {
	Authenticated bool
	User          *acl.User
	Config        *Config
	Tx            *TxState
	ReplicaPort   int
}

// TxState holds the transaction state for a connection.
type TxState struct {
	Active bool
	Dirty  bool
	Queue  []QueuedCommand
}

// QueuedCommand is a command stored during MULTI.
type QueuedCommand struct {
	Cmd  string
	Args []string
}

type Config struct {
	RequirePass string
	AclEngine   *acl.Engine
	Replication *replication.State
}

// Handler function type
type Handler func(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error)

// GetHandlers returns the map of command handlers
func GetHandlers() map[string]Handler {
	return map[string]Handler{
		"PING":        ping,
		"ECHO":        echo,
		"SET":         set,
		"GET":         get,
		"INCR":        incr,
		"RPUSH":       rpush,
		"LRANGE":      lrange,
		"LPUSH":       lpush,
		"BLPOP":       blpop,
		"LLEN":        llen,
		"LPOP":        lpop,
		"TYPE":        typeCmd,
		"XADD":        xadd,
		"XRANGE":      xrange,
		"XREAD":       xread,
		"CONFIG":      config,
		"KEYS":        keys,
		"SUBSCRIBE":   subscribe,
		"PUBLISH":     publish,
		"UNSUBSCRIBE": unsubscribe,
		"ZADD":        zadd,
		"ZRANGE":      zrange,
		"ZCARD":       zcard,
		"ZREM":        zrem,
		"ZRANK":       zrank,
		"ZSCORE":      zscore,
		"GEOADD":      geoadd,
		"GEODIST":     geodist,
		"GEOPOS":      geopos,
		"GEOHASH":     geohash,
		"GEOSEARCH":   geosearch,
		"AUTH":        auth,
		"ACL":         aclCmd,
		"INFO":        info,
		"REPLCONF":    replconf,
		"PSYNC":       psync,
		"WAIT":        waitCmd,
	}
}
