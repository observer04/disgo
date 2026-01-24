package store

import (
	"errors"
	"strconv"
	"time"
)

// SetWithTTL stores the key-value pair in the Kv store with an optional TTL
func (k *Kv) SetWithTTL(key, value string, ttl time.Duration) {
	var deadline time.Time
	if ttl > 0 {
		deadline = time.Now().Add(ttl)
	}
	k.SetWithDeadline(key, value, deadline)
}

// SetWithDeadline stores key-value with an absolute expiration time.
// If deadline is zero, the key persists.
// If deadline is in the past, the key is deleted (or not set).
func (k *Kv) SetWithDeadline(key, value string, deadline time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if !deadline.IsZero() && !deadline.After(time.Now()) {
		// Expired.
		delete(k.data, key)
		delete(k.exp, key)
		return
	}

	k.data[key] = value
	if !deadline.IsZero() {
		k.exp[key] = deadline
	} else {
		delete(k.exp, key)
	}
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

// Incr increments the integer value of a key and returns the new value.
func (k *Kv) Incr(key string) (int64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Check for expiration
	if expTime, ok := k.exp[key]; ok {
		if time.Now().After(expTime) {
			delete(k.data, key)
			delete(k.exp, key)
		}
	}

	if _, ok := k.lists[key]; ok {
		return 0, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := k.sortedSets[key]; ok {
		return 0, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := k.streams[key]; ok {
		return 0, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	current, ok := k.data[key]
	var n int64
	if ok {
		parsed, err := strconv.ParseInt(current, 10, 64)
		if err != nil {
			return 0, errors.New("ERR value is not an integer or out of range")
		}
		n = parsed
	}

	n++
	k.data[key] = strconv.FormatInt(n, 10)
	return n, nil
}
