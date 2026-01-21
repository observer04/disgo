package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

const (
	OpAux        = 0xFA
	OpResizeDB   = 0xFB
	OpExpireTime = 0xFD
	OpExpireMs   = 0xFC
	OpSelectDB   = 0xFE
	OpEOF        = 0xFF

	TypeString = 0
)

func Load(dir, dbfilename string, kv *store.Kv) error {
	if dir == "" || dbfilename == "" {
		return nil
	}
	path := filepath.Join(dir, dbfilename)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil // No RDB file, start empty
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)

	// 1. Header: "REDIS"
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	if string(header) != "REDIS" {
		return fmt.Errorf("invalid file format")
	}

	// Skip version (4 bytes)
	if _, err := r.Discard(4); err != nil {
		return err
	}

	var expiry *time.Time

	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}

		switch b {
		case OpEOF:
			return nil
		case OpSelectDB:
			// Read DB number (length encoded)
			_, _, err := readLength(r)
			if err != nil {
				return err
			}
			// We only support DB 0 for now, so ignore the value
		case OpResizeDB:
			// Read db_size and expires_size
			_, _, err := readLength(r) // db hash table size
			if err != nil {
				return err
			}
			_, _, err = readLength(r) // expiry hash table size
			if err != nil {
				return err
			}
		case OpAux:
			// Read key and value (strings)
			_, err := readString(r) // key
			if err != nil {
				return err
			}
			_, err = readString(r) // value
			if err != nil {
				return err
			}
		case OpExpireTime:
			// 4 byte timestamp (seconds)
			var t uint32
			if err := binary.Read(r, binary.LittleEndian, &t); err != nil {
				return err
			}
			tm := time.Unix(int64(t), 0)
			expiry = &tm
		case OpExpireMs:
			// 8 byte timestamp (milliseconds)
			var t uint64
			if err := binary.Read(r, binary.LittleEndian, &t); err != nil {
				return err
			}
			tm := time.UnixMilli(int64(t))
			expiry = &tm
		default:
			// Value Type
			valType := b
			key, err := readString(r)
			if err != nil {
				return err
			}

			if valType == TypeString {
				val, err := readString(r)
				if err != nil {
					return err
				}
				if expiry != nil {
					kv.SetWithTTL(key, val, time.Until(*expiry))
				} else {
					kv.Set(key, val)
				}
			} else {
				// Skip other types for now?
				// To implement robust skipping, we'd need to know how to parse them.
				// For this challenge, we might only encounter strings.
				return fmt.Errorf("unsupported value type: %d", valType)
			}
			expiry = nil // reset expiry
		}
	}
}

// readLength reads a length-encoded integer from the reader.
// Returns the length, true if special encoding, and error.
func readLength(r *bufio.Reader) (int, bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, false, err
	}

	// First 2 bits determine the format
	switch (b & 0xC0) >> 6 {
	case 0:
		// 00xxxxxx: 6 bit length
		return int(b & 0x3F), false, nil
	case 1:
		// 01xxxxxx: 14 bit length
		next, err := r.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return int(b&0x3F)<<8 | int(next), false, nil
	case 2:
		// 10xxxxxx: 32 bit length (next 4 bytes)
		// Only if b == 0x80 (discarded?) or 0x81?
		// Actually spec says: 10...... -> The remaining 6 bits are discarded.
		// The next 4 bytes are the length (big endian).
		var l uint32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return 0, false, err
		}
		return int(l), false, nil
	case 3:
		// 11xxxxxx: Special encoding
		// The remaining 6 bits specify the type of encoding.
		return int(b & 0x3F), true, nil
	}
	return 0, false, nil
}

func readString(r *bufio.Reader) (string, error) {
	length, isSpecial, err := readLength(r)
	if err != nil {
		return "", err
	}

	if isSpecial {
		switch length {
		case 0: // 8 bit integer
			b, err := r.ReadByte()
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", int8(b)), nil
		case 1: // 16 bit integer
			var i int16
			if err := binary.Read(r, binary.LittleEndian, &i); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", i), nil
		case 2: // 32 bit integer
			var i int32
			if err := binary.Read(r, binary.LittleEndian, &i); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", i), nil
		default:
			return "", fmt.Errorf("unsupported special encoding: %d", length)
		}
	}

	// Normal string
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
