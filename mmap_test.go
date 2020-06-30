package bigqueue

import (
	"os"
	"strings"
	"testing"
)

func Test_MmapWriteAt(t *testing.T) {
	path := Tempfile() + "/test.dat"
	defer os.Remove(path)

	db := &DB{
		path:            path,
		InitialMmapSize: 128 * 1024,
		opened:          true,
	}

	err := db.Open(0666)
	if err != nil {
		t.Error(err)
	}

	defer db.Close()

	s := "hello xiemalin"
	db.file.Write([]byte(s))

	v := db.data[:len(s)]
	if strings.Compare(s, string(v)) != 0 {
		t.Error("except string is ", s, " but actually is", string(v))
	}

}

func Test_MmapWriteAt1(t *testing.T) {
	path := Tempfile() + "/test.dat"
	defer os.Remove(path)
	db := &DB{
		path:            path,
		InitialMmapSize: 128 * 1024,
		opened:          true,
	}

	err := db.Open(0666)
	if err != nil {
		t.Error(err)
	}

	s := "hello xiemalin"
	bb := []byte(s)
	for i := 0; i < len(bb); i++ {
		db.data[i] = bb[i]
	}

	db.Close()

	db = &DB{
		path:            path,
		InitialMmapSize: 128 * 1024,
		opened:          true,
	}
	//re-open
	err = db.Open(0666)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	s = "hello xiemalin"
	v := db.data[:len(s)]
	if strings.Compare(s, string(v)) != 0 {
		t.Error("except string is ", s, " but actually is", string(v))
	}
}
