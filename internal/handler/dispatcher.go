package handler

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/replication"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// Dispatcher handles the command execution logic, including state validation.
// It returns a resp.Value which can be a success response or a Redis error (resp.Error).
// It returns a Go error only for internal failures that shouldn't be sent to client (or handled by caller).
func Dispatch(cmd string, args []string, kv *store.Kv, msgCh chan interface{}, handlers map[string]Handler, state *ConnectionState) (resp.Value, error) {
	cmd = strings.ToUpper(cmd)

	if state.Tx == nil {
		state.Tx = &TxState{}
	}

	// Auth check
	if !state.Authenticated && cmd != "AUTH" && cmd != "QUIT" {
		return resp.Error("NOAUTH Authentication required."), nil
	}

	// Check Pub/Sub mode restrictions
	subCount := kv.GetSubscriptionCount(msgCh)
	if subCount > 0 {
		allowed := map[string]bool{
			"SUBSCRIBE":   true,
			"UNSUBSCRIBE": true,
			"PING":        true,
			"QUIT":        true,
			"AUTH":        true, // Allow AUTH in pubsub? Usually no, but let's check. Redis: "A client subscribed to one or more channels should not issue commands, other than..."
			// AUTH is not in the allowed list for PubSub usually.
		}
		if !allowed[cmd] {
			return resp.Error(fmt.Sprintf("ERR Can't execute '%s': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context", strings.ToLower(cmd))), nil
		}
	}

	// Transaction control commands
	switch cmd {
	case "MULTI":
		if len(args) != 0 {
			return resp.Error("ERR wrong number of arguments for 'multi' command"), nil
		}
		if state.Tx.Active {
			return resp.Error("ERR MULTI calls can not be nested"), nil
		}
		state.Tx.Active = true
		state.Tx.Dirty = false
		state.Tx.Queue = nil
		return resp.SimpleString("OK"), nil
	case "DISCARD":
		if len(args) != 0 {
			return resp.Error("ERR wrong number of arguments for 'discard' command"), nil
		}
		if !state.Tx.Active {
			return resp.Error("ERR DISCARD without MULTI"), nil
		}
		state.Tx.Active = false
		state.Tx.Dirty = false
		state.Tx.Queue = nil
		return resp.SimpleString("OK"), nil
	case "EXEC":
		if len(args) != 0 {
			return resp.Error("ERR wrong number of arguments for 'exec' command"), nil
		}
		if !state.Tx.Active {
			return resp.Error("ERR EXEC without MULTI"), nil
		}
		if state.Tx.Dirty {
			state.Tx.Active = false
			state.Tx.Dirty = false
			state.Tx.Queue = nil
			return resp.Error("EXECABORT Transaction discarded because of previous errors."), nil
		}

		queued := append([]QueuedCommand(nil), state.Tx.Queue...)
		state.Tx.Active = false
		state.Tx.Dirty = false
		state.Tx.Queue = nil

		if len(queued) == 0 {
			return resp.Array{}, nil
		}

		responses := make(resp.Array, 0, len(queued))
		for _, qc := range queued {
			val, err := Dispatch(qc.Cmd, qc.Args, kv, msgCh, handlers, state)
			if err != nil {
				return nil, err
			}
			if val == nil {
				val = resp.NullBulkString{}
			}
			responses = append(responses, val)
		}
		return responses, nil
	}

	// Queue commands if we're inside MULTI
	if state.Tx.Active {
		if _, ok := handlers[cmd]; !ok {
			state.Tx.Dirty = true
			return resp.Error(fmt.Sprintf("ERR unknown command '%s'", cmd)), nil
		}
		state.Tx.Queue = append(state.Tx.Queue, QueuedCommand{Cmd: cmd, Args: args})
		return resp.SimpleString("QUEUED"), nil
	}

	cmdHandler, ok := handlers[cmd]
	if !ok {
		return resp.Error(fmt.Sprintf("ERR unknown command '%s'", cmd)), nil
	}

	val, err := cmdHandler(args, kv, msgCh, state)
	if err != nil {
		return resp.Error(err.Error()), nil
	}

	if state.Config != nil && state.Config.Replication != nil {
		repl := state.Config.Replication
		if repl.Role() == replication.RoleMaster && !repl.IsReplicaChannel(msgCh) && isWriteCommand(cmd) {
			_, repErr := repl.PropagateCommand(cmd, args)
			if repErr != nil {
				return nil, repErr
			}
		}
	}
	return val, nil
}

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "INCR", "RPUSH", "LPUSH", "LPOP", "ZADD", "ZREM", "XADD", "GEOADD":
		return true
	default:
		return false
	}
}
