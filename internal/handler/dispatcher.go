package handler

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// Dispatcher handles the command execution logic, including state validation.
// It returns a resp.Value which can be a success response or a Redis error (resp.Error).
// It returns a Go error only for internal failures that shouldn't be sent to client (or handled by caller).
func Dispatch(cmd string, args []string, kv *store.Kv, msgCh chan interface{}, handlers map[string]Handler, state *ConnectionState) (resp.Value, error) {
	cmd = strings.ToUpper(cmd)

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

	cmdHandler, ok := handlers[cmd]
	if !ok {
		return resp.Error(fmt.Sprintf("ERR unknown command '%s'", cmd)), nil
	}

	val, err := cmdHandler(args, kv, msgCh, state)
	if err != nil {
		return resp.Error(err.Error()), nil
	}
	return val, nil
}