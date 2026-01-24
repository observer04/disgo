package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/acl"
	"github.com/codecrafters-io/redis-starter-go/internal/handler"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
	"github.com/codecrafters-io/redis-starter-go/internal/replication"
	"github.com/codecrafters-io/redis-starter-go/internal/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

func startReplicaClient(host string, port int, localPort int, kv *store.Kv, handlers map[string]handler.Handler, aclEngine *acl.Engine, replState *replication.State) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("replication: failed to connect to master %s: %v", addr, err)
		return
	}

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	sendCmd := func(cmd string, args ...string) error {
		arr := resp.Array{resp.BulkString(cmd)}
		for _, arg := range args {
			arr = append(arr, resp.BulkString(arg))
		}
		if err := resp.Write(w, arr); err != nil {
			return err
		}
		return w.Flush()
	}

	readSimple := func() (string, error) {
		line, err := readLineCRLF(r)
		if err != nil {
			return "", err
		}
		if len(line) == 0 || line[0] != '+' {
			return "", errors.New("expected simple string")
		}
		return line[1:], nil
	}

	// Handshake
	if err := sendCmd("PING"); err != nil {
		log.Printf("replication: failed to send PING: %v", err)
		return
	}
	if _, err := readSimple(); err != nil {
		log.Printf("replication: failed to read PONG: %v", err)
		return
	}

	if localPort == 0 {
		localPort = 6379
	}
	if err := sendCmd("REPLCONF", "listening-port", strconv.Itoa(localPort)); err != nil {
		log.Printf("replication: failed to send REPLCONF listening-port: %v", err)
		return
	}
	_, _ = readSimple()

	if err := sendCmd("REPLCONF", "capa", "psync2"); err != nil {
		log.Printf("replication: failed to send REPLCONF capa: %v", err)
		return
	}
	_, _ = readSimple()

	if err := sendCmd("PSYNC", "?", "-1"); err != nil {
		log.Printf("replication: failed to send PSYNC: %v", err)
		return
	}

	line, err := readLineCRLF(r)
	if err != nil {
		log.Printf("replication: failed to read FULLRESYNC: %v", err)
		return
	}
	if !strings.HasPrefix(line, "+FULLRESYNC ") {
		log.Printf("replication: unexpected FULLRESYNC response: %s", line)
		return
	}
	parts := strings.Split(line[1:], " ")
	if len(parts) >= 3 {
		if offset, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			replState.AdvanceOffset(offset - replState.Offset())
		}
	}

	rdbBytes, err := readBulkBytes(r)
	if err != nil {
		log.Printf("replication: failed to read RDB: %v", err)
		return
	}
	if len(rdbBytes) > 0 {
		_ = rdb.LoadFromReader(bytes.NewReader(rdbBytes), kv)
	}

	defaultUser, _ := aclEngine.GetUser("default")
	connState := &handler.ConnectionState{
		Authenticated: true,
		User:          defaultUser,
		Config: &handler.Config{
			AclEngine:   aclEngine,
			Replication: replState,
		},
		Tx: &handler.TxState{},
	}

	msgCh := make(chan interface{}, 1)
	for {
		args, err := resp.Read(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("replication: read error: %v", err)
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		cmdArgs := args[1:]

		payload, err := replication.EncodeCommand(cmd, cmdArgs)
		if err == nil {
			replState.AdvanceOffset(int64(len(payload)))
		}

		if cmd == "REPLCONF" && len(cmdArgs) >= 2 && strings.ToUpper(cmdArgs[0]) == "GETACK" {
			offset := replState.Offset()
			_ = sendCmd("REPLCONF", "ACK", strconv.FormatInt(offset, 10))
			continue
		}

		_, _ = handler.Dispatch(cmd, cmdArgs, kv, msgCh, handlers, connState)
	}
}

func readLineCRLF(r *bufio.Reader) (string, error) {
	b, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return "", errors.New("line does not end with CRLF")
	}
	return string(b[:len(b)-2]), nil
}

func readBulkBytes(r *bufio.Reader) ([]byte, error) {
	line, err := readLineCRLF(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, errors.New("expected bulk string")
	}
	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
