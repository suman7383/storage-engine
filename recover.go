package storageengine

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/wal"
)

// TODO: WAL Replay logic goes here
func (db *DB) replayWAL() error {
	walSegments, err := scanWalDirectory(db.walDir)
	if err != nil {
		log.Fatal("error scanning WAL directory, err: ", err)
	}

	var maxSeq uint64 = 0

	for _, segment := range walSegments {
		fd, err := os.Open(segment.Path)
		if err != nil {
			return err
		}

		reader := wal.NewReader(fd)

		isWalCorrupt := false

		// Read the WAL entries
		for reader.HasNext() {
			record, eof, err := reader.Next()
			if eof {
				break
			}

			if err != nil {
				// Corrupt wal file
				isWalCorrupt = true
				log.Printf("found corrupt wal file, %+v, err: %v\n", segment, err)
				break
			}

			seq, err := db.activeMem.Apply(record.Key, record.Value, record.Seq)
			if err != nil {
				log.Fatal("could not load record to memtable, err: ", err)
			}

			if seq > maxSeq {
				maxSeq = seq
			}
		}

		// Close the fd
		fd.Close()

		if isWalCorrupt {
			break
		}
	}

	db.nextSeq = maxSeq + 1

	return nil
}

func buildInternalKey(rec *wal.WALRecord) internalkey.InternalKey {
	return internalkey.NewInternalKey(rec.Key, rec.Seq, internalkey.KeyType(rec.Op))
}

func scanWalDirectory(dir string) ([]wal.WALSegmentMeta, error) {
	var segments []wal.WALSegmentMeta

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}

		idStr := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			continue
		}

		segments = append(segments, wal.WALSegmentMeta{
			Id:   id,
			Path: filepath.Join(dir, name),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Id < segments[j].Id
	})

	return segments, nil
}
