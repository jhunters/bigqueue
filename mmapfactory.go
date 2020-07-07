package bigqueue

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// DBFactory is used to manupilate mulitple data files by index number
type DBFactory struct {
	lockMap map[int64]*sync.Mutex

	dbMap map[int64]*DB

	filePrefix string

	fileSuffix string

	lock sync.Mutex

	filePath string

	InitialMmapSize int
}

func (f *DBFactory) acquireDB(index int64) (*DB, error) {
	db := f.dbMap[index]
	if db != nil {
		return db, nil
	}

	// add map lock
	f.lock.Lock()
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

	err := db.Open(0666)
	if err != nil {
		return nil, err
	}
	f.dbMap[index] = db
	return db, nil
}

func (f *DBFactory) getFilePath(index int64) string {
	return f.filePath + "/" + GetFileName(f.filePrefix, f.fileSuffix, index)
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

	f.dbMap = nil
	f.lockMap = nil

	return nil

}

func (f *DBFactory) removeBeforeIndex(index int64) error {

	f.lock.Lock()
	defer f.lock.Unlock()

	for k, v := range f.dbMap {
		if int64(k) < index {
			log.Println("Do delete index db file by gc. no=", k)

			v.Close()
			os.Remove(f.getFilePath(k))
			delete(f.dbMap, k)
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
