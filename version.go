package storageengine

import "github.com/suman7383/storage-engine/sstable"

type Version struct {
	levels [][]*sstable.SstReader
}
