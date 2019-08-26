package simpledb

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// DB is the struct used to interact with the database
type DB struct {
	offset int64
	file   *os.File
	idx    *index
	writer sync.Mutex
}

func intToBytes(i int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func bytesToInt(b []byte) int {
	return int(binary.LittleEndian.Uint64(b))
}

// Connect creates a new instance of a DB pointing to the given file with the given filename on disk
func Connect(filename string) (*DB, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	offset := fi.Size()
	idx, err := createIndex(filename + ".index")
	if err != nil {
		return nil, err
	}

	return &DB{
		file:   f,
		idx:    idx,
		offset: offset,
	}, nil
}

// Close closes the db
func (db *DB) Close() {
	db.file.Close()
	db.idx.close()
}

// Put adds a new record to the db
func (db *DB) Put(k string, v []byte) error {
	db.writer.Lock()
	defer db.writer.Unlock()

	v = append(intToBytes(len(v)), v...)
	bytes, err := db.file.Write(v)
	if err != nil {
		return err
	}

	if err := db.idx.put(k, db.offset); err != nil {
		return err
	}

	db.offset += int64(bytes)

	return nil
}

// Get retrieves the value that corresponds to the given key or returns an error if
// the key is not found or something goes wrong reading the file
func (db *DB) Get(k string) ([]byte, error) {
	offset, ok := db.idx.get(k)
	if !ok {
		return nil, fmt.Errorf("Key not found: %s", k)
	}

	b := make([]byte, 8)
	if _, err := db.file.ReadAt(b, offset); err != nil {
		return nil, err
	}

	b = make([]byte, bytesToInt(b))
	if _, err := db.file.ReadAt(b, offset+8); err != nil {
		return nil, err
	}

	return b, nil
}
