package handler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/replication"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

// info returns INFO output. Currently only supports replication section.
func info(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	section := ""
	if len(args) > 0 {
		section = strings.ToLower(args[0])
	}

	repl := state.Config.Replication
	if repl == nil {
		return resp.BulkString("# Replication\r\nrole:master\r\nmaster_replid:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\n"), nil
	}

	if section != "" && section != "replication" {
		return resp.BulkString(""), nil
	}

	role := repl.Role()
	if role == replication.RoleMaster {
		info := fmt.Sprintf("# Replication\r\nrole:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\nconnected_slaves:%d\r\n", repl.ReplID(), repl.Offset(), repl.ReplicaCount())
		return resp.BulkString(info), nil
	}

	host, port := repl.MasterAddr()
	info := fmt.Sprintf("# Replication\r\nrole:slave\r\nmaster_host:%s\r\nmaster_port:%d\r\nmaster_link_status:up\r\nslave_repl_offset:%d\r\n", host, port, repl.Offset())
	return resp.BulkString(info), nil
}

// replconf handles replication configuration commands from replicas.
func replconf(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("ERR wrong number of arguments for 'replconf' command")
	}

	repl := state.Config.Replication
	if repl == nil {
		return resp.SimpleString("OK"), nil
	}

	subCmd := strings.ToUpper(args[0])
	switch subCmd {
	case "LISTENING-PORT":
		if len(args) != 2 {
			return nil, errors.New("ERR wrong number of arguments for 'replconf' command")
		}
		port, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.New("ERR invalid port")
		}
		state.ReplicaPort = port
		return resp.SimpleString("OK"), nil
	case "CAPA":
		return resp.SimpleString("OK"), nil
	case "ACK":
		if len(args) != 2 {
			return nil, errors.New("ERR wrong number of arguments for 'replconf' command")
		}
		offset, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return nil, errors.New("ERR invalid offset")
		}
		if repl.Role() == replication.RoleMaster {
			repl.Ack(msgCh, offset)
		}
		return nil, nil
	default:
		return resp.SimpleString("OK"), nil
	}
}

// psync initiates replication with a replica.
func psync(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	repl := state.Config.Replication
	if repl == nil || repl.Role() != replication.RoleMaster {
		return resp.Error("ERR PSYNC not supported"), nil
	}

	repl.RegisterReplica(msgCh, state.ReplicaPort)

	fullResync := fmt.Sprintf("FULLRESYNC %s %d", repl.ReplID(), repl.Offset())
	msgCh <- resp.SimpleString(fullResync)

	emptyRDB := []byte("REDIS0009\xFF")
	msgCh <- resp.RawBulk(emptyRDB)
	return nil, nil
}

// waitCmd implements WAIT numreplicas timeout
func waitCmd(args []string, kv *store.Kv, msgCh chan interface{}, state *ConnectionState) (resp.Value, error) {
	if len(args) != 2 {
		return nil, errors.New("ERR wrong number of arguments for 'wait' command")
	}

	repl := state.Config.Replication
	if repl == nil || repl.Role() != replication.RoleMaster {
		return resp.Integer(0), nil
	}

	numReplicas, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, errors.New("ERR invalid numreplicas")
	}
	timeoutMs, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("ERR invalid timeout")
	}

	currentOffset := repl.Offset()
	acked := repl.AckedCount(currentOffset)
	if currentOffset == 0 {
		return resp.Integer(int64(acked)), nil
	}
	if acked >= numReplicas || timeoutMs == 0 {
		return resp.Integer(int64(acked)), nil
	}

	targetOffset, err := repl.RequestAck()
	if err != nil {
		return nil, err
	}

	acked = repl.WaitForAcks(numReplicas, targetOffset, time.Duration(timeoutMs)*time.Millisecond)
	return resp.Integer(int64(acked)), nil
}
