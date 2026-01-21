package store

import (
	"errors"
)

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

	id, err := stream.Add(id, values)
	if err != nil {
		return "", err
	}

	// Notify waiters
	if waiters, ok := k.streamWaiters[key]; ok {
		for _, ch := range waiters { // send to all waiters
			// Non-blocking send
			select {
			case ch <- key:
			default: // if channel is full, skip
			}
		}
	}

	return id, nil
}

// XRange returns a range of entries from a stream.
func (k *Kv) XRange(key, start, end string) ([]StreamEntry, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Check type> key shouldnt have anything except stream
	if _, ok := k.data[key]; ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := k.lists[key]; ok {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream, ok := k.streams[key]
	if !ok {
		return []StreamEntry{}, nil
	}

	return stream.Range(start, end)
}

// XRead returns entries from multiple streams starting from the given IDs.
func (k *Kv) XRead(streams map[string]string) (map[string][]StreamEntry, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	result := make(map[string][]StreamEntry)

	for key, startID := range streams {
		// Check type
		if _, ok := k.data[key]; ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if _, ok := k.lists[key]; ok {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		stream, ok := k.streams[key]
		if !ok {
			continue // Key doesn't exist, skip
		}

		// Handle "$" special ID (returns no entries, just meant for blocking later,
		// but if called in non-blocking XREAD, it simply returns empty)
		if startID == "$" {
			// In XREAD (non-blocking), $ theoretically means "messages strictly greater than max ID"
			// which is nothing. So we skip.
			// However, the caller usually resolves "$" to the actual last ID before calling this if blocking.
			// For non-blocking, XREAD STREAMS key $ returns empty.
			continue
		}

		entries, err := stream.Read(startID)
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			result[key] = entries
		}
	}

	return result, nil
}

// NewStreamWaiter registers a new waiter channel for the given keys.
func (k *Kv) NewStreamWaiter(keys []string) chan string {
	k.mu.Lock()
	defer k.mu.Unlock()

	ch := make(chan string, len(keys)) // Buffered to avoid blocking sender;
	for _, key := range keys {
		k.streamWaiters[key] = append(k.streamWaiters[key], ch)
	}
	return ch
}

// RemoveStreamWaiter removes the waiter channel for the given keys.
func (k *Kv) RemoveStreamWaiter(keys []string, ch chan string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	//
	for _, key := range keys {
		waiters := k.streamWaiters[key]
		for i, w := range waiters {
			if w == ch {
				k.streamWaiters[key] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		// Clean up empty key entries if desired, but not strictly necessary
		if len(k.streamWaiters[key]) == 0 {
			delete(k.streamWaiters, key)
		}
	}
}

// GetLastID returns the last ID of a stream. Returns "0-0" if empty or key doesn't exist.
func (k *Kv) GetLastID(key string) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, ok := k.data[key]; ok {
		return "", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if _, ok := k.lists[key]; ok {
		return "", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream, ok := k.streams[key]
	if !ok {
		return "0-0", nil // Treat missing key as empty stream
	}
	return stream.GetLastID(), nil
}
