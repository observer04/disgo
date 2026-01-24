package handler

import (
	"errors"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func aclCmd(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("ERR wrong number of arguments for 'acl' command")
	}

	subCmd := strings.ToUpper(args[0])

	switch subCmd {
	case "WHOAMI":
		return aclWhoami(args[1:], state)
	case "GETUSER":
		return aclGetUser(args[1:], state)
	case "LIST", "USERS":
		// Optional: List users
		return aclList(state)
	default:
		return nil, errors.New("ERR unknown subcommand or wrong number of arguments for 'acl'. Try ACL HELP.")
	}
}

func aclWhoami(args []string, state *ConnectionState) (resp.Value, error) {
	if len(args) != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'acl|whoami' command")
	}
	user := state.User
	if user == nil {
		// Should not happen if connection initialized correctly
		return resp.BulkString("default"), nil 
	}
	return resp.BulkString(user.Name), nil
}

func aclGetUser(args []string, state *ConnectionState) (resp.Value, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'acl|getuser' command")
	}
	username := args[0]
	
	user, ok := state.Config.AclEngine.GetUser(username)
	if !ok {
		return resp.NullBulkString{}, nil // Redis returns nil if user doesn't exist? Or empty array?
		// "If the user does not exist, (nil) is returned."
	}

	// Returns array of properties
	// flags, passwords, commands, keys, channels, selectors...
	res := resp.Array{}

	res = append(res, resp.BulkString("flags"))
	flags := resp.Array{}
	if user.Enabled {
		flags = append(flags, resp.BulkString("on"))
	} else {
		flags = append(flags, resp.BulkString("off"))
	}
	// Simplified flags
	res = append(res, flags)

	res = append(res, resp.BulkString("passwords"))
	passwords := resp.Array{}
	if user.Password != "" {
		// Do not return actual password usually?
		// ACL GETUSER returns hashes.
		// Since we store plain text (for this exercise), we might return "+<password>"?
		// Redis: "passwords" -> list of password hashes.
		// If we store plaintext, let's just return "+<plaintext>" to mimic representation.
		passwords = append(passwords, resp.BulkString("+"+user.Password))
	} else {
		passwords = append(passwords, resp.BulkString("nopass"))
	}
	res = append(res, passwords)

	res = append(res, resp.BulkString("commands"))
	res = append(res, resp.BulkString("+@all")) // Placeholder

	res = append(res, resp.BulkString("keys"))
	res = append(res, resp.Array{resp.BulkString("~*")}) // Placeholder (all keys)

	// Minimal implementation for now
	return res, nil
}

func aclList(state *ConnectionState) (resp.Value, error) {
	users := state.Config.AclEngine.ListUsers()
	res := make(resp.Array, len(users))
	for i, u := range users {
		res[i] = resp.BulkString(u)
	}
	return res, nil
}
