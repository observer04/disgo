package store

import (
	"errors"
	"sync"
	"time"
)

// Kv is a simple in-memory key-value store with mutex for concurrency safety.
type Kv struct {
	mu      sync.Mutex
	data    map[string]string
	exp     map[string]time.Time
	lists   map[string][]string
	streams map[string]*Stream // key to Stream mapping
	// waiters holds channels for clients blocked on BLPOP for a given key.
	// When an element is pushed to a list with waiting clients, the server
	// will deliver the element to the longest-waiting client instead of
	// appending it to the list.
	waiters map[string][]chan string
}

// constructor function for Kv
func NewKv() *Kv {
	return &Kv{
		data:    make(map[string]string),
		exp:     make(map[string]time.Time),
		lists:   make(map[string][]string),
		streams: make(map[string]*Stream),
		waiters: make(map[string][]chan string),
	}
}

// Set stores the key-value pair in the Kv store with an optional TTL
func (k *Kv) SetWithTTL(key, value string, ttl time.Duration) {
	k.mu.Lock()
	k.data[key] = value
	if ttl > 0 {
		k.exp[key] = time.Now().Add(ttl)
	} else {
		delete(k.exp, key)
	}
	k.mu.Unlock()
}

// without expiration
func (k *Kv) Set(key, value string) {
	k.SetWithTTL(key, value, 0)
}

func (k *Kv) Get(key string) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Check for expiration
	if expTime, ok := k.exp[key]; ok {
		if time.Now().After(expTime) {
			// Key has expired
			delete(k.data, key)
			delete(k.exp, key)
			return "", false
		}
	}
	val, ok := k.data[key]
	return val, ok
}

// list operations:
// RPUSH : append values to the list stored at key
func (k *Kv) RPush(key string, values ...string) int {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Append all values first so the returned length reflects the list
	// size immediately after the RPUSH. Then deliver elements to any
	// waiters (FIFO) by popping from the list and sending the value.

	// append values
	k.lists[key] = append(k.lists[key], values...)
	pushedLen := len(k.lists[key])

	// deliver to waiters while both waiters and list items exist
	for len(k.waiters[key]) > 0 && len(k.lists[key]) > 0 {
		ch := k.waiters[key][0]
		k.waiters[key] = k.waiters[key][1:]
		// pop first element
		val := k.lists[key][0]
		k.lists[key] = k.lists[key][1:]
		// deliver without blocking; channel is buffered but use goroutine as fallback
		select {
		case ch <- val:
		default:
			go func(c chan string, v string) { c <- v }(ch, val)
		}
	}
	return pushedLen
}

// LRANGE: get elements from list stored at key
func (k *Kv) LRange(key string, start, stop int) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	list, ok := k.lists[key]
	if !ok {
		return []string{}, nil
	}
	n := len(list)

	// convert negative indices to absolute positions
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}

	// if negative index is out of range, treat as 0
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = 0
	}

	if start >= n || start > stop {
		return []string{}, nil
	}
	if stop >= n {
		stop = n - 1
	}
	return list[start : stop+1], nil
}

// LPush: prepend values to the list stored at key
func (k *Kv) LPush(key string, values ...string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	// Prepend values in reverse order so the given order is preserved
	for i := len(values) - 1; i >= 0; i-- {
		k.lists[key] = append([]string{values[i]}, k.lists[key]...)
	}
	return len(k.lists[key])
}

// LLen: get length of list stored at key
func (k *Kv) LLen(key string) int64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return int64(len(k.lists[key]))
}

// LPop: remove and return the first element OR the n first elements if n is provided
func (k *Kv) LPop(key string, n int) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	list, ok := k.lists[key]
	if !ok || len(list) == 0 {
		return nil, errors.New("list is empty or does not exist")
	}
	if n <= 0 {
		n = 1
	}
	if n > len(list) {
		n = len(list)
	}
	vals := make([]string, n)
	copy(vals, list[:n])
	k.lists[key] = list[n:]
	return vals, nil
}

// BlockingPop tries to pop a value immediately. If not found, it waits.
// Returns value, true if popped immediately.
// Returns channel, false if waiting.
func (k *Kv) BlockingPop(key string) (string, <-chan string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if list, ok := k.lists[key]; ok && len(list) > 0 {
		val := list[0]
		k.lists[key] = list[1:]
		return val, nil, true
	}

	// No element available: set up a waiter
	ch := make(chan string, 1)
	k.waiters[key] = append(k.waiters[key], ch)
	return "", ch, false
}

// RemoveWaiter removes the channel from the waiters list for the key
func (k *Kv) RemoveWaiter(key string, ch <-chan string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	waiters := k.waiters[key]
	for i, w := range waiters {
		if w == ch { // Note: this comparison might need casting if types strictly don't match, but here ch came from append
			k.waiters[key] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
}

// Type returns the string representation of the value's type.
// Returns "string", "list", "stream" (future), or "none".
func (k *Kv) Type(key string) string {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Check for expiration
	if expTime, ok := k.exp[key]; ok {
		if time.Now().After(expTime) { // key has expired
			delete(k.data, key)
			delete(k.lists, key)
			delete(k.exp, key)
			return "none"
		}
	}

	if _, ok := k.data[key]; ok {
		return "string"
	}
	if _, ok := k.lists[key]; ok {
		return "list"
	}
	if _, ok := k.streams[key]; ok {
		return "stream"
	}
	return "none"
}

// XAdd appends an entry to a stream. Creates the stream if it doesn't exist.
func (k *Kv) XAdd(key, id string, values []string) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Check for collision with other types
	if _, ok := k.data[key]; ok {
		return "", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := k.lists[key]; ok {
		return "", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream, ok := k.streams[key]
	if !ok {
		stream = NewStream()
		k.streams[key] = stream
	}

	return stream.Add(id, values)
}

// RunExpirationLoop handles the background expiration of keys
func (k *Kv) RunExpirationLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		k.mu.Lock()
		for key, exp := range k.exp {
			if now.After(exp) { // key has expired
				delete(k.data, key)
				delete(k.lists, key)
				delete(k.exp, key)
			}
		}
		k.mu.Unlock()
	}
}
