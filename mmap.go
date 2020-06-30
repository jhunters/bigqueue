package bigqueue

import (
	"fmt"
	"os"
	"sync"
)

// maxMapSize represents the largest mmap size supported by Bolt.
const maxMapSize = 0x7FFFFFFF // 2GB

// maxAllocSize is the size used when creating array pointers.
const maxAllocSize = 0xFFFFFFF

// Are unaligned load/stores broken on this arch?
var brokenUnaligned = false

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	MmapFlags int

	path string
	file *os.File

	data *[maxMapSize]byte

	InitialMmapSize int

	opened bool

	dataref []byte // mmap'ed readonly, write throws SEGV

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	mmaplock sync.RWMutex // Protects mmap access during remapping.
}

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {

	buf := make([]byte, db.InitialMmapSize)

	// Write the buffer to our data file.
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

func (db *DB) Open(mode os.FileMode) error {

	flag := os.O_RDWR
	// Open data file and separate sync handler for metadata writes.
	var err error

	if db.file, err = os.OpenFile(db.Path(), flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return err
	}

	// Default values for test hooks
	db.ops.writeAt = db.file.WriteAt

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
		return err
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return err
		}
	}

	// Memory map the data file.
	if err := db.mmap(db.InitialMmapSize); err != nil {
		_ = db.close()
		return err
	}

	// Mark the database as opened and return.
	return nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}

	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	if err := mmap(db, size); err != nil {
		return err
	}

	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// Close releases all resources.
func (db *DB) Close() error {
	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	fdatasync(db)

	// Clear ops.
	db.ops.writeAt = nil

	// Close the mmap.
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	if db.file != nil {
		// Close the file descriptor.
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}
