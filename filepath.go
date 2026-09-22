package storageengine

import "path/filepath"

func getSstFilePath(storageDir, fileName string) string {
	return filepath.Join(storageDir, "sst", fileName+".sst")
}
