package replication

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

type Role string

const (
	RoleMaster  Role = "master"
	RoleReplica Role = "slave"
)

type Replica struct {
	ch            chan interface{}
	listeningPort int
	ackOffset     int64
}

type State struct {
	mu         sync.Mutex
	role       Role
	replID     string
	offset     int64
	replicas   map[chan interface{}]*Replica
	ackCh      chan struct{}
	masterHost string
	masterPort int
}

func NewMaster() *State {
	return &State{
		role:     RoleMaster,
		replID:   newReplID(),
		replicas: make(map[chan interface{}]*Replica),
		ackCh:    make(chan struct{}, 1),
	}
}

func NewReplica(masterHost string, masterPort int) *State {
	return &State{
		role:       RoleReplica,
		masterHost: masterHost,
		masterPort: masterPort,
		ackCh:      make(chan struct{}, 1),
	}
}

func newReplID() string {
	b := make([]byte, 20)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *State) Role() Role {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.role
}

func (s *State) ReplID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replID
}

func (s *State) Offset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offset
}

func (s *State) MasterAddr() (string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.masterHost, s.masterPort
}

func (s *State) RegisterReplica(ch chan interface{}, listeningPort int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replicas == nil {
		s.replicas = make(map[chan interface{}]*Replica)
	}
	if _, ok := s.replicas[ch]; ok {
		return
	}
	s.replicas[ch] = &Replica{ch: ch, listeningPort: listeningPort}
}

func (s *State) UnregisterReplica(ch chan interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.replicas, ch)
}

func (s *State) IsReplicaChannel(ch chan interface{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.replicas[ch]
	return ok
}

func (s *State) ReplicaCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.replicas)
}

func (s *State) Ack(ch chan interface{}, offset int64) {
	s.mu.Lock()
	if rep, ok := s.replicas[ch]; ok {
		rep.ackOffset = offset
	}
	s.mu.Unlock()
	select {
	case s.ackCh <- struct{}{}:
	default:
	}
}

func (s *State) AckedCount(targetOffset int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, rep := range s.replicas {
		if rep.ackOffset >= targetOffset {
			count++
		}
	}
	return count
}

func (s *State) AdvanceOffset(delta int64) {
	s.mu.Lock()
	s.offset += delta
	s.mu.Unlock()
}

func (s *State) PropagateCommand(cmd string, args []string) (int64, error) {
	s.mu.Lock()
	if s.role != RoleMaster || len(s.replicas) == 0 {
		s.mu.Unlock()
		return s.offset, nil
	}
	payload, err := EncodeCommand(cmd, args)
	if err != nil {
		s.mu.Unlock()
		return s.offset, err
	}
	s.offset += int64(len(payload))
	replicas := make([]*Replica, 0, len(s.replicas))
	for _, rep := range s.replicas {
		replicas = append(replicas, rep)
	}
	s.mu.Unlock()

	val := resp.Array{resp.BulkString(cmd)}
	for _, arg := range args {
		val = append(val, resp.BulkString(arg))
	}
	for _, rep := range replicas {
		rep.ch <- val
	}
	return s.Offset(), nil
}

func (s *State) RequestAck() (int64, error) {
	cmd := "REPLCONF"
	args := []string{"GETACK", "*"}
	payload, err := EncodeCommand(cmd, args)
	if err != nil {
		return s.Offset(), err
	}

	s.mu.Lock()
	if s.role != RoleMaster || len(s.replicas) == 0 {
		s.mu.Unlock()
		return s.offset, nil
	}
	s.offset += int64(len(payload))
	replicas := make([]*Replica, 0, len(s.replicas))
	for _, rep := range s.replicas {
		replicas = append(replicas, rep)
	}
	s.mu.Unlock()

	val := resp.Array{resp.BulkString(cmd), resp.BulkString("GETACK"), resp.BulkString("*")}
	for _, rep := range replicas {
		rep.ch <- val
	}
	return s.Offset(), nil
}

func (s *State) WaitForAcks(targetCount int, targetOffset int64, timeout time.Duration) int {
	if targetCount <= 0 {
		return s.AckedCount(targetOffset)
	}
	count := s.AckedCount(targetOffset)
	if count >= targetCount || timeout == 0 {
		return count
	}

	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return s.AckedCount(targetOffset)
		}
		select {
		case <-s.ackCh:
			count = s.AckedCount(targetOffset)
			if count >= targetCount {
				return count
			}
		case <-time.After(remaining):
			return s.AckedCount(targetOffset)
		}
	}
}
