package handler

import (
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func subscribe(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) == 0 {
		return nil, nil // Or error?
	}

	for _, channel := range args {
		kv.Subscribe(channel, msgCh)
		
		// Send message to msgCh
		msg := resp.Array{
			resp.BulkString("subscribe"),
			resp.BulkString(channel),
			resp.Integer(1), // TODO: Fix this count
		}
		msgCh <- msg
	}
	return nil, nil // Already sent responses
}

func publish(args []string, kv *store.Kv, msgCh chan interface{}) (resp.Value, error) {
	if len(args) != 2 {
		return nil, nil // Error
	}
	channel := args[0]
	message := args[1]

	// Format the message for subscribers: ["message", channel, message]
	payload := resp.Array{
		resp.BulkString("message"),
		resp.BulkString(channel),
		resp.BulkString(message),
	}

	count := kv.Publish(channel, payload)
	return resp.Integer(count), nil
}