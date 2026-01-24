package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/acl"
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
	case "SETUSER":
		return aclSetUser(args[1:], state)
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
		return resp.NullBulkString{}, nil
	}

	res := resp.Array{}

	res = append(res, resp.BulkString("flags"))
	flags := resp.Array{}
	if user.Enabled {
		flags = append(flags, resp.BulkString("on"))
	} else {
		flags = append(flags, resp.BulkString("off"))
	}
	if user.Password == "" {
		flags = append(flags, resp.BulkString("nopass"))
	}
	res = append(res, flags)

	res = append(res, resp.BulkString("passwords"))
	passwords := resp.Array{}
	if user.Password != "" {
		hash := sha256.Sum256([]byte(user.Password))
		hashStr := hex.EncodeToString(hash[:])
		passwords = append(passwords, resp.BulkString(hashStr))
	}
	res = append(res, passwords)

	res = append(res, resp.BulkString("commands"))
	res = append(res, resp.BulkString("+@all")) 

	res = append(res, resp.BulkString("keys"))
	res = append(res, resp.Array{resp.BulkString("~*")}) 

	return res, nil
}

func aclSetUser(args []string, state *ConnectionState) (resp.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("ERR wrong number of arguments for 'acl|setuser' command")
	}
	username := args[0]
	rules := args[1:]

	var err error
	state.Config.AclEngine.SetUser(username, func(u *acl.User) {
		for _, rule := range rules {
			ruleLower := strings.ToLower(rule)
			if ruleLower == "on" {
				u.Enabled = true
			} else if ruleLower == "off" {
				u.Enabled = false
			} else if ruleLower == "nopass" {
				u.Password = ""
			} else if strings.HasPrefix(rule, ">") {
				u.Password = rule[1:]
			} else if ruleLower == "+@all" {
				// We don't implement full permission logic yet, but we accept the rule
			} else if strings.HasPrefix(ruleLower, "~") {
				// Key pattern, ignore for now
			} else {
				// Unknown rule, but we'll ignore for this simplified version
			}
		}
	})

	if err != nil {
		return nil, err
	}

	return resp.SimpleString("OK"), nil
}

func aclList(state *ConnectionState) (resp.Value, error) {
	users := state.Config.AclEngine.ListUsers()
	res := make(resp.Array, len(users))
	for i, u := range users {
		res[i] = resp.BulkString(u)
	}
	return res, nil
}