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

// WAL Replay logic
func (db *DB) replayWAL() (maxSeq uint64, err error) {
	log.Print("[WAL] replay in-process")

	wdir := filepath.Join(db.storageDir, "wal")
	err = os.MkdirAll(wdir, 0755)
	if err != nil {
		log.Fatalln("Error creating WAL directory:", err)
		return 0, err
	}

	walSegments, err := scanWalDirectory(wdir)
	if err != nil {
		log.Fatal("error scanning WAL directory, err: ", err)
	}

	log.Printf("[WAL] scanning done. segments: %v", walSegments)

	for _, segment := range walSegments {
		fd, err := os.Open(segment.Path)
		if err != nil {
			return 0, err
		}

		reader := wal.NewReader(fd)

		isWalCorrupt := false

		// Read the WAL entries
		for reader.HasNext() {
			// log.Print("Has next record: TRUE")
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

			// log.Printf("[WAL] record. [SEQ]: %v, [KEY]: %v, [VALUE]: %v, [OP]: %v",
			// 	record.Seq,
			// 	string(record.Key),
			// 	string(record.Value),
			// 	record.Op)

			seq, err := db.activeMem.Apply(record.Key, record.Value, record.Seq, record.Op)
			if err != nil {
				log.Fatal("could not load record to memtable, err: ", err)
			}

			if seq > maxSeq {
				maxSeq = seq
			}

			if segment.StartSeq > segment.EndSeq {
				segment.StartSeq = seq
			}

			segment.EndSeq = seq
		}

		db.walSegments = append(db.walSegments, segment)

		// Close the fd
		fd.Close()

		if isWalCorrupt {
			break
		}
	}

	return maxSeq, nil
}

func buildInternalKey(rec *wal.WALRecord) internalkey.InternalKey {
	return internalkey.NewInternalKey(rec.Key, rec.Seq, rec.Op)
}

func scanWalDirectory(dir string) ([]wal.WALSegmentMeta, error) {
	log.Print("[WAL] scanning wal files...")
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

			StartSeq: 1,
			EndSeq:   0,
			State:    wal.WALSealed,
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Id < segments[j].Id
	})

	return segments, nil
}
