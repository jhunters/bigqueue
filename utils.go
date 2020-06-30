package bigqueue

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// _assert will panic with a given formatted message if the given condition is false.
func Assert(condition bool, message string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+message, v...))
	}
}

func warn(v ...interface{}) {
	fmt.Fprintln(os.Stderr, v...)
}

func warnf(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GetFileName(prefix string, suffix string, index int64) string {
	return prefix + strconv.Itoa(int(index)) + suffix
}

//int64 to byte array
func IntToBytes(n int64) []byte {
	x := int64(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//byte to int64
func BytesToInt(b []byte) int64 {
	bytesBuffer := bytes.NewBuffer(b)

	var x int64
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int64(x)
}

// bytes to int32
func BytesToInt32(b []byte) int32 {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int32(x)
}

func Mod(val int64, bits int) int64 {
	return val - ((val >> uint(bits)) << uint(bits))
}

func GetFiles(pathname string) (*list.List, error) {

	files := list.New()
	rd, err := ioutil.ReadDir(pathname)
	for _, fi := range rd {
		if fi.IsDir() {
			continue
		} else {
			files.PushBack(fi.Name())
		}
	}
	return files, err
}

func RemoveFiles(pathname string) error {
	list, err := GetFiles(pathname)
	if err != nil {
		return err
	}
	for i := list.Front(); i != nil; i = i.Next() {
		fn := fmt.Sprintf("%v", i.Value)
		err = os.Remove(pathname + "/" + fn)
		if err != nil {
			return err
		}
	}
	return nil
}
