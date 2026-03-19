package storageengine

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/suman7383/storage-engine/internalkey"
)

type ManifestRecord struct {
	Operation   ManifestOperation
	Level       int
	FileID      string
	SmallestKey internalkey.InternalKey
	LargestKey  internalkey.InternalKey
}

type ManifestOperation string

const (
	Add    ManifestOperation = "ADD"
	Delete ManifestOperation = "DEL"
)

var ErrInvalidManifestOperation = errors.New("Invalid Operation")
var ErrCorruptManifestRecord = errors.New("Corrupt manifest record found")
var ErrParsingManifestRecord = errors.New("Could not parse a manifest record")

func ParseManifestOperation(op []byte) (ManifestOperation, error) {
	mop := ManifestOperation(string(op))

	switch mop {
	case Add, Delete:
		return mop, nil
	default:
		return "", ErrInvalidManifestOperation
	}
}

type manifest struct {
	fd *os.File
}

const delimiter = byte('\n')
const separator = byte(' ')

func (m *manifest) Add(rec ManifestRecord) error {
	bw := bufio.NewWriter(m.fd)

	// Write Operation
	bw.Write([]byte(rec.Operation + " "))

	// Level
	bw.Write([]byte(strconv.Itoa(rec.Level) + " "))

	// fileID
	bw.Write([]byte(rec.FileID + " "))

	// Smallest key
	bw.Write(rec.SmallestKey)
	bw.WriteByte(separator)

	// Largest Key
	bw.Write(rec.LargestKey)
	bw.WriteByte(separator)

	// delimiter
	bw.WriteByte(delimiter)

	err := bw.Flush()
	if err != nil {
		return err
	}

	return m.fd.Sync()
}

func (m *manifest) FSync() error {
	return m.fd.Sync()
}

type ManifestIterator struct {
	reader        bufio.Reader
	currentRecord ManifestRecord
	err           error
}

func (m *manifest) NewIterator() *ManifestIterator {
	return &ManifestIterator{
		reader: *bufio.NewReader(m.fd),
		err:    nil,
	}
}

// Next() scans the next record and stores it internally to returned on call to Value().
// It stores the record that can be accessed by calling Value()
func (mi *ManifestIterator) Next() bool {
	// Decode the record
	line, err := mi.reader.ReadBytes(delimiter)

	// Only return false if err is EOF and the line read is empty
	if err == io.EOF && len(line) == 0 {
		mi.err = err
		return false
	}

	fields := bytes.Fields(line)
	i := 0

	operation, err := ParseManifestOperation(fields[i])
	if err != nil {
		mi.err = err
		return false
	}

	// ADD operation should have 5 fields
	if operation == Add && len(fields) != 5 {
		log.Printf("[MANIFEST] found Add record with fields: %v", len(fields))
		mi.err = ErrCorruptManifestRecord
		return false
	}

	// DELETE operation should have 3 fields
	if operation == Delete && len(fields) != 3 {
		mi.err = ErrCorruptManifestRecord
		return false
	}

	i++

	levelStr := string(fields[i])
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		mi.err = ErrParsingManifestRecord
		return false
	}

	i++
	fileID := string(fields[i])

	i++

	if operation == Delete {
		mi.currentRecord = ManifestRecord{
			Operation: operation,
			Level:     level,
			FileID:    fileID,
		}

		return true
	}

	smallestKey := fields[i]
	i++

	largestKey := fields[i]

	mi.currentRecord = ManifestRecord{
		Operation:   operation,
		Level:       level,
		FileID:      fileID,
		SmallestKey: smallestKey,
		LargestKey:  largestKey,
	}

	return true
}

// Value() returns the record obtained from the Next() call
func (mi *ManifestIterator) Value() ManifestRecord {
	return mi.currentRecord
}

// Err() returns the error occured during call to Next() if any
func (mi *ManifestIterator) Err() error {
	return mi.err
}
