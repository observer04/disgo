package store

import (
	"sync"
	"time"
)

// Kv is a simple in-memory key-value store with mutex for concurrency safety.
type Kv struct {
	mu      sync.Mutex
	data    map[string]string
	exp     map[string]time.Time
	lists   map[string][]string
	sortedSets map[string]*SortedSet
	streams map[string]*Stream // key to Stream mapping
	// waiters holds channels for clients blocked on BLPOP for a given key.
	// When an element is pushed to a list with waiting clients, the server
	// will deliver the element to the longest-waiting client instead of
	// appending it to the list.
	waiters map[string][]chan string
	// streamWaiters holds channels for clients blocked on XREAD for a given key.
	// The channel receives the key name when a new entry is added.
	streamWaiters map[string][]chan string

	// Configuration
	config map[string]string

	// Pub/Sub
	subs map[string]map[chan interface{}]struct{}
}

// constructor function for Kv
func NewKv(dir, dbfilename string) *Kv {
	return &Kv{
		data:          make(map[string]string),
		exp:           make(map[string]time.Time),
		lists:         make(map[string][]string),
		sortedSets:    make(map[string]*SortedSet),
		streams:       make(map[string]*Stream),
		waiters:       make(map[string][]chan string),
		streamWaiters: make(map[string][]chan string),
		config: map[string]string{
			"dir":        dir,
			"dbfilename": dbfilename,
		},
		subs: make(map[string]map[chan interface{}]struct{}),
	}
}

// GetConfig returns the value of a configuration parameter
func (k *Kv) GetConfig(key string) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.config[key]
}

// Keys returns all keys matching the pattern.
// Currently only supports "*"
func (k *Kv) Keys(pattern string) []string {
	k.mu.Lock()
	defer k.mu.Unlock()

	var keys []string
	for key := range k.data { // only string keys for now; lists and streams can be added similarly
		// simple implementation for "*"
		if pattern == "*" {
			keys = append(keys, key)
		} else {
			// fallback/todo: implement glob matching
			if key == pattern {
				keys = append(keys, key)
			}
		}
	}
	return keys
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
			delete(k.sortedSets, key)
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
	if _, ok := k.sortedSets[key]; ok {
		return "zset"
	}
	if _, ok := k.streams[key]; ok {
		return "stream"
	}
	return "none"
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
				delete(k.sortedSets, key)
				delete(k.exp, key)
			}
		}
		k.mu.Unlock()
	}
}
