package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// StreamEntry represents a single entry in a Redis Stream. ex: 1526985058136-0 and key-value pairs.
type StreamEntry struct {
	ID     string
	Values []string // Flat list of Key, Value, Key, Value...
}

// Stream represents a Redis Stream data structure.
type Stream struct {
	Entries []StreamEntry //multiple entries ex: [{ID:1526985058136-0, Values:[key1 val1 key2 val2]} ...]
}

func NewStream() *Stream {
	return &Stream{
		Entries: make([]StreamEntry, 0), //returns empty slice of StreamEntry as pointer to Stream
	}
}

// getLastID returns the timestamp and sequence of the last entry.
// Returns 0, 0 if empty (which conceptually acts as 0-0 being the "previous" ID).
func (s *Stream) getLastID() (int64, int64) {
	if len(s.Entries) == 0 {
		return 0, 0
	}
	last := s.Entries[len(s.Entries)-1]
	return parseIDUnsafe(last.ID)
}

func parseIDUnsafe(id string) (int64, int64) {
	parts := strings.Split(id, "-")
	//ms and seq : example: 1526985058136-0 > 1526985058136 , 0
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	return ms, seq
}

// parseID parses a stream ID string into its millisecond and sequence components.
func parseID(id string) (int64, int64, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("ERR value is not an integer or out of range")
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, errors.New("ERR value is not an integer or out of range")
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, errors.New("ERR value is not an integer or out of range")
	}
	return ms, seq, nil
}

// Add appends a new entry to the stream.
// Handles ID validation and auto-generation ('*').
func (s *Stream) Add(idStr string, values []string) (string, error) {
	if idStr == "0-0" {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	lastMs, lastSeq := s.getLastID()
	var newMs, newSeq int64

	if idStr == "*" {
		// Auto-generate ID
		newMs = time.Now().UnixMilli()
		// Ensure we don't go backward in time relative to the last entry
		if newMs < lastMs {
			newMs = lastMs
		}
		if newMs == lastMs {
			newSeq = lastSeq + 1
		} else {
			newSeq = 0
		}
		// Edge case: if generated ID is 0-0, bump to 0-1
		if newMs == 0 && newSeq == 0 {
			newSeq = 1
		}

	} else if strings.HasSuffix(idStr, "-*") {
		// Partial ID: "ms-*"
		msStr := strings.TrimSuffix(idStr, "-*")
		var err error
		newMs, err = strconv.ParseInt(msStr, 10, 64)
		if err != nil {
			return "", errors.New("ERR value is not an integer or out of range")
		}
		if newMs < lastMs {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if newMs == lastMs {
			newSeq = lastSeq + 1
		} else {
			newSeq = 0
		}
		// Edge case: 0-* must resolve to at least 0-1
		if newMs == 0 && newSeq == 0 {
			newSeq = 1
		}
	} else {
		// Explicit ID
		var err error
		newMs, newSeq, err = parseID(idStr)
		if err != nil {
			return "", err
		}
		if newMs == 0 && newSeq == 0 {
			return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
		}
		// Validate monotonic increase
		if newMs < lastMs || (newMs == lastMs && newSeq <= lastSeq) {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}
	finalID := fmt.Sprintf("%d-%d", newMs, newSeq)
	entry := StreamEntry{
		ID:     finalID,
		Values: values,
	}
	s.Entries = append(s.Entries, entry)
	return finalID, nil
}
