# Storage Engine

A lightweight, high-performance Key-Value storage engine built from scratch in Go, implementing a Log-Structured Merge-Tree (LSM-Tree) architecture.

## 📖 Overview

This project is a deep dive into the internals of modern databases. It implements the core components of an LSM-tree based storage engine, similar to LevelDB or RocksDB, focusing on durability, performance, and crash recovery.

### Key Components

- **Write-Ahead Log (WAL)**: Ensures data durability by logging every operation before applying it to the memtable.
- **Memtable**: An in-memory SkipList that provides $O(\log N)$ performance for writes and reads.
- **SSTables (Sorted String Tables)**: Persistent, sorted, and immutable files on disk for long-term storage.
- **Manifest**: A specialized log that tracks the state of the system, including active SSTables and their key ranges across different levels.
- **Versioning**: Each key-value pair is associated with a sequence number, enabling snapshot isolation and MVCC-like behavior.

## Features

- [x] **CRUD Operations**: `Put`, `Get`, and `Delete` support.
- [x] **Persistence**: Automatic flushing of Memtables to SSTables.
- [x] **Crash Recovery**: Full state restoration from WAL and Manifest files after a restart.
- [x] **Internal Key Management**: Handles sequence numbers and operation types (Put/Delete) transparently.

## 📅 Future Plans

- [ ] **SST Compaction**: Multi-level compaction to reclaim space and optimize read performance.
- [ ] **WAL Compaction**: Background log cleaning and checkpointing to keep WAL sizes manageable.
- [ ] **Bloom Filters**: To significantly reduce unnecessary disk I/O during lookups.
- [ ] **Binary Manifest**: Transition from text-based to a more efficient binary format for faster metadata operations.

## 🚀 Getting Started

### Installation

```bash
go get github.com/suman7383/storage-engine
```

### Usage Example

```go
package main

import (
	"log"
	storageengine "github.com/suman7383/storage-engine"
)

func main() {
	// Configure options
	opts := storageengine.Options{
		StorageDir:            "./db_data",
		MemtableMaxSize:       16 * 1024 * 1024, // 16MB
		MaxImmutableMemtables: 5,
	}

	// Initialize and open the database
	db := storageengine.NewDB(opts)
	db.Open()

	// Put a value
	key := []byte("greeting")
	value := []byte("Hello, Storage Engine!")
	db.Put(key, value)

	// Get a value
	val, ok := db.Get(key)
	if ok {
		log.Printf("Found: %s", string(val))
	}

	// Delete a value
	db.Delete(key)
}
```

## 🏗 Architecture

The engine follows a classic LSM-Tree write path:
1. **WAL**: The operation is appended to an on-disk log.
2. **Memtable**: The data is inserted into an in-memory SkipList.
3. **Flush**: Once the Memtable reaches a size threshold, it is frozen and flushed to disk as a Level-0 SSTable.
4. **SSTable Lookup**: Reads check the active Memtable, then frozen Memtables, and finally the SSTables (from L0 downwards).

## 📚 API Reference

| Method | Description |
| :--- | :--- |
| `NewDB(Options)` | Creates a new DB instance with the specified configuration. |
| `Open()` | Initializes the engine, replays WAL/Manifest, and prepares for operations. |
| `Put(key, value)` | Inserts or updates a key-value pair. |
| `Get(key)` | Retrieves the most recent value for a given key. |
| `Delete(key)` | Marks a key as deleted (appends a tombstone). |

## ⚖️ License

Distributed under the MIT License. See `LICENSE` for more information.
