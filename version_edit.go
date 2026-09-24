package storageengine

import (
	"errors"
	"os"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/sstable"
)

type VersionRecord ManifestRecord

type VersionEdit []VersionRecord

func (ve VersionEdit) Add(rec VersionRecord) {
	ve = append(ve, rec)
}

var ErrInvalidVersionEdit = errors.New("invalid version edit")

// Contains the sst readers that were added as a result of this version edit
type preparedAdds struct {
	reader *sstable.SstReader

	// meta-data
	fileID      string
	smallestKey internalkey.InternalKey
	largestKey  internalkey.InternalKey
	level       int
}

// TODO: This does not work currently, FIX IT.
func (ve *VersionEdit) Apply(v *Version, storageDir string) (*Version, error) {
	// Validate the versionEdit is valid on the current version v
	if err := ve.Validate(v); err != nil {
		return nil, err
	}

	// Prepare all ADD readers
	adds := make([][]*preparedAdds, len(v.levels)) // [level][...*preparedAdds]

	// keeps track of errors during preparation of adding files
	var errPrepareAdd error

	for _, edit := range *ve {
		if edit.Operation != Add {
			continue
		}

		// Open the sst file
		filePath := getSstFilePath(storageDir, edit.FileID)
		fd, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
		if err != nil {
			logger.Error("Failed to open sst file during version apply", "error", err, "source", "versionEdit.Apply")
			errPrepareAdd = err
			break
		}

		fileInfo, err := fd.Stat()
		if err != nil {
			logger.Error("Failed to get file info during version apply", "error", err, "source", "versionEdit.Apply")

			// Clean up the file descriptor
			fd.Close()
			errPrepareAdd = err
			break
		}

		// Create new sst reader
		sstReader, err := sstable.NewSstReader(fd, fileInfo.Size(), edit.FileID, edit.SmallestKey, edit.LargestKey)
		if err != nil {
			logger.Error("Failed to create sst reader from file descriptor during version apply", "error", err, "source", "versionEdit.Apply")

			// Clean up the file descriptor
			fd.Close()
			errPrepareAdd = err
			break
		}

		// Add the sst file to the adds slice
		adds[edit.Level] = append(adds[edit.Level], &preparedAdds{
			reader:      sstReader,
			fileID:      edit.FileID,
			smallestKey: edit.SmallestKey,
			largestKey:  edit.LargestKey,
			level:       edit.Level,
		})
	}

	// Check if there was an error during preparation, clean up and return
	if errPrepareAdd != nil {
		// Release ownership of the sstReaders that were successfully created
		for _, level := range adds {
			for _, preparedAdd := range level {
				// Release refs of every prepared add reader
				preparedAdd.reader.Release()
			}
		}

		return nil, errPrepareAdd
	}

	// Clone the version
	cloned := v.Clone()

	// Apply all the ADD readers
	for _, level := range adds {
		for _, pa := range level {
			cloned.levels[pa.level] = append(cloned.levels[pa.level], pa.reader)
		}
	}

	// Apply all the DELETE operations
	for _, edit := range *ve {
		if edit.Operation == Delete {
			// Find the file and remove it from the cloned version
			for i, sstReader := range cloned.levels[edit.Level] {
				if sstReader.GetFileId() == edit.FileID {
					// Release ownership of the sstReader
					sstReader.Release()
					// Remove the sstReader from the cloned version
					cloned.levels[edit.Level] = append(cloned.levels[edit.Level][:i], cloned.levels[edit.Level][i+1:]...)
					break
				}
			}

		}
	}

	return cloned, nil
}

// Validate checks if the versionEdit is valid for the given version v
func (ve *VersionEdit) Validate(v *Version) error {
	// Go through all the edits and validate them
	for _, edit := range *ve {

		// Level must be valid
		// TODO: Change len(v.levels) to v.config.maxLevel later
		if edit.Level < 0 || edit.Level >= len(v.levels) {
			logger.Error("Invalid level", "level", edit.Level, "source", "versionEdit.Validate")
			return ErrInvalidVersionEdit
		}

		// FileID must be valid(non-empty)
		if edit.FileID == "" {
			logger.Error("Invalid fileID", "fileID", edit.FileID, "source", "versionEdit.Validate")
			return ErrInvalidVersionEdit
		}

		switch edit.Operation {
		case Add:
			// File must not already exist in the version
			for _, existingSSTReader := range v.levels[edit.Level] {
				if existingSSTReader.GetFileId() == edit.FileID {
					logger.Error("File already exists", "fileID", edit.FileID, "level", edit.Level, "source", "versionEdit.Validate")
					return ErrInvalidVersionEdit
				}
			}

			// SmallestKey and LargestKey should be present
			if edit.SmallestKey == nil || edit.LargestKey == nil {
				logger.Error("SmallestKey or LargestKey is nil", "smallestKey", edit.SmallestKey, "largestKey", edit.LargestKey, "source", "versionEdit.Validate")
				return ErrInvalidVersionEdit
			}

			// SmallestKey <= LargestKey
			if edit.SmallestKey.Compare(edit.LargestKey) > 0 {
				logger.Error("SmallestKey is greater than LargestKey", "smallestKey", edit.SmallestKey, "largestKey", edit.LargestKey, "source", "versionEdit.Validate")
				return ErrInvalidVersionEdit
			}

		case Delete:
			// File must exist in the version
			found := false
			for _, existingSSTReader := range v.levels[edit.Level] {
				if existingSSTReader.GetFileId() == edit.FileID {
					found = true
					break
				}
			}
			if !found {
				logger.Error("File not found", "fileID", edit.FileID, "level", edit.Level, "source", "versionEdit.Validate")
				return ErrInvalidVersionEdit
			}
		default:
			logger.Error("Invalid operation", "operation", edit.Operation, "source", "versionEdit.Validate")
			return ErrInvalidVersionEdit
		}

	}

	return nil
}
