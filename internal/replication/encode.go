package replication

import "github.com/codecrafters-io/redis-starter-go/internal/resp"

// EncodeCommand builds RESP bytes for a command and arguments.
func EncodeCommand(cmd string, args []string) ([]byte, error) {
	arr := resp.Array{resp.BulkString(cmd)}
	for _, arg := range args {
		arr = append(arr, resp.BulkString(arg))
	}
	return resp.Encode(arr)
}
