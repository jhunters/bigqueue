package bigqueue

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// DBFactory is used to manupilate mulitple data files by index number
type DBFactory struct {
	lockMap map[int64]*sync.Mutex

	// DB mapping with file index no
	dbMap map[int64]*DB

	filePrefix string

	fileSuffix string

	lock sync.Mutex

	filePath string

	InitialMmapSize int
}

func (f *DBFactory) acquireDB(index int64) (*DB, error) {
	// add map lock
	f.lock.Lock()
	db := f.dbMap[index]
	if db != nil {
		f.lock.Unlock()
		return db, nil
	}

	lock := f.lockMap[index]
	if lock == nil {
		lock = &(sync.Mutex{})
		f.lockMap[index] = lock
	}
	defer func() {
		delete(f.lockMap, index)
	}()
	f.lock.Unlock()

	// lock by index
	lock.Lock()
	defer lock.Unlock()

	db = &DB{
		path:            f.getFilePath(index),
		InitialMmapSize: f.InitialMmapSize,
		opened:          true,
	}

	err := db.Open(defaultFileMode)
	if err != nil {
		return nil, err
	}
	f.dbMap[index] = db
	return db, nil
}

func (f *DBFactory) getFilePath(index int64) string {
	return filepath.Join(f.filePath, GetFileName(f.filePrefix, f.fileSuffix, index))
}

// Close all data files
func (f *DBFactory) Close() error {
	if f.dbMap != nil {
		for k, v := range f.dbMap {
			err := v.Close()
			if err != nil {
				log.Println("Close DB from map failed. ", k, err)
			}
		}
	}

	// set to the emtpy map
	f.dbMap = make(map[int64]*DB)
	f.lockMap = make(map[int64]*sync.Mutex)

	return nil

}

func (f *DBFactory) removeBeforeIndex(index int64) error {

	f.lock.Lock()
	defer f.lock.Unlock()

	for idx, db := range f.dbMap {
		if int64(idx) < index {
			log.Println("Do delete index db file by gc. no=", idx)

			db.Close()
			os.Remove(f.getFilePath(idx))
			delete(f.dbMap, idx)
		}
	}

	// double check delete file
	files, err := GetFiles(f.filePath)
	if err != nil {
		return err
	}
	for i := files.Front(); i != nil; i = i.Next() {
		fn := fmt.Sprintf("%v", i.Value)
		if strings.HasSuffix(fn, f.fileSuffix) {
			fin := f.getFileIndex(fn)
			if fin >= 0 && int64(fin) < index {
				log.Println("Do delete index db file by gc. no=", fin)
				os.Remove(f.getFilePath(fin))
			}
		}

	}

	return nil
}

func (f *DBFactory) getFileIndex(fn string) int64 {
	beginIndex := strings.LastIndex(fn, "-")
	beginIndex = beginIndex + 1

	endIndex := strings.LastIndex(fn, f.fileSuffix)

	sIndex, err := strconv.Atoi(fn[beginIndex:endIndex])
	if err != nil {
		return -1
	}

	return int64(sIndex)
}
