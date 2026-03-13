package storageengine

import (
	"bufio"
	"os"
	"strconv"
)

type ManifestRecord struct {
	Operation   ManifestOperation
	Level       int
	FileID      int
	SmallestKey []byte
	LargestKey  []byte
}

type ManifestOperation string

const (
	Add    ManifestOperation = "ADD"
	Delete ManifestOperation = "DEL"
)

type manifest struct {
	fd *os.File
}

const delimiter = byte('\n')
const separator = byte(' ')

func (m manifest) Add(rec ManifestRecord) error {
	bw := bufio.NewWriter(m.fd)

	// Write Operation
	bw.Write([]byte(rec.Operation + " "))

	// Level
	bw.Write([]byte(strconv.Itoa(rec.Level) + " "))

	// fileID
	bw.Write([]byte(strconv.Itoa(rec.FileID) + " "))

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
