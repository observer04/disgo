package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Value interface{}

type SimpleString string
type Error string
type BulkString string
type Integer int64
type Array []Value
type NullArray struct{}
type NullBulkString struct{}
type RawBulk []byte

// validate and read a line ending with CRLF
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

func Read(r *bufio.Reader) ([]string, error) {
	peek, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	if peek[0] == '*' {
		// Array
		header, err := readLineCRLF(r)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, errors.New("invalid array length")
		}
		args := make([]string, 0, count) // Preallocate slice with capacity, length 0, to avoid nil slice
		for i := 0; i < count; i++ {
			line, err := readLineCRLF(r)
			if err != nil {
				return nil, err
			}
			if line[0] != '$' {
				return nil, errors.New("expected bulk string")
			}
			length, err := strconv.Atoi(line[1:])
			if err != nil {
				return nil, errors.New("invalid bulk string length")
			}
			// if length is -1, it's a null bulk string, args append empty string
			if length < 0 {
				args = append(args, "")
				continue
			}
			buf := make([]byte, length+2) // +2 for CRLF
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			if !bytes.HasSuffix(buf, []byte("\r\n")) {
				return nil, errors.New("bulk string does not end with CRLF")
			}

			args = append(args, string(buf[:length]))

		}
		return args, nil
	}

	//inline command mode without RESP array for nc clients
	line, err := readLineCRLF(r)
	if err != nil {
		return nil, err
	}
	args := strings.Fields(line)
	return args, nil

}

// write RESP value to writer
func Write(w *bufio.Writer, val Value) error {
	switch v := val.(type) { // reason is that Go does not allow switch on types directly so we use type assertion
	case NullBulkString:
		// Null bulk string
		_, err := w.WriteString("$-1\r\n")
		return err
	case NullArray:
		_, err := w.WriteString("*-1\r\n")
		return err
	case SimpleString:
		if _, err := w.WriteString(fmt.Sprintf("+%s\r\n", string(v))); err != nil { //eg: +OK\r\n
			return err
		}
		return nil
	case Error:
		if _, err := w.WriteString(fmt.Sprintf("-%s\r\n", string(v))); err != nil {
			return err
		}
		return nil

	case BulkString:

		if _, err := w.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)); err != nil { //eg: $3\r\nfoo\r\n
			return err
		}
		return nil
	case RawBulk:
		if _, err := w.WriteString(fmt.Sprintf("$%d\r\n", len(v))); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
		return nil
	case Integer:
		if _, err := w.WriteString(fmt.Sprintf(":%d\r\n", v)); err != nil {
			return err
		}
		return nil
	case Array:
		if _, err := w.WriteString(fmt.Sprintf("*%d\r\n", len(v))); err != nil {
			return err
		}
		for _, elem := range v {
			if err := Write(w, elem); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("unsupported RESP type")
	}
}

// Encode serializes a RESP value into bytes.
func Encode(val Value) ([]byte, error) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if err := Write(w, val); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
