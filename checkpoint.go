package storageengine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Update updates the checkpoint atomically to the passed value.
func updateCheckpoint(storageDir string, seq uint64) error {
	cDir := filepath.Join(storageDir, "checkpoint")

	err := os.MkdirAll(cDir, 0755)
	if err != nil {
		return err
	}

	finalPath := filepath.Join(cDir, "checkpoint")
	tempPath := filepath.Join(cDir, "temp")

	seqStr := strconv.FormatUint(seq, 10)

	fd, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	_, err = fd.WriteString(seqStr)
	if err != nil {
		return err
	}

	err = fd.Sync()
	if err != nil {
		fd.Close()
		return err
	}

	fd.Close()

	// Rename
	if err := os.Rename(tempPath, finalPath); err != nil {
		return err
	}

	dirFd, err := os.Open(cDir)
	if err != nil {
		return err
	}
	defer dirFd.Close()

	return dirFd.Sync()
}

func readCheckpoint(storageDir string) (seq uint64, err error) {
	cDir := filepath.Join(storageDir, "checkpoint")

	err = os.MkdirAll(cDir, 0755)
	if err != nil {
		return 0, err
	}

	filePath := filepath.Join(cDir, "checkpoint")
	fd, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	fmt.Fscanf(fd, "%d", &seq)
	return seq, nil
}
