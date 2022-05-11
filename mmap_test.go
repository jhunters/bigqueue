package bigqueue

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_MmapWriteAt(t *testing.T) {
	path := Tempfile() + "/test.dat"
	defer os.Remove(path)

	Convey("Test_MmapWriteAt", t, func() {
		db := &DB{
			path:            path,
			InitialMmapSize: 128 * 1024,
			opened:          true,
		}

		err := db.Open(0666)
		So(err, ShouldBeNil)

		defer db.Close()

		s := "hello xiemalin"
		db.file.Write([]byte(s))

		v := db.data[:len(s)]
		So(s, ShouldEqual, string(v))
	})

}

func Test_MmapWriteAt1(t *testing.T) {
	path := Tempfile() + "/test.dat"
	defer os.Remove(path)

	Convey("Test_MmapWriteAt1", t, func() {
		db := &DB{
			path:            path,
			InitialMmapSize: 128 * 1024,
			opened:          true,
		}

		err := db.Open(0666)
		So(err, ShouldBeNil)

		s := "hello xiemalin"
		bb := []byte(s)
		// for i := 0; i < len(bb); i++ {
		// 	db.data[i] = bb[i]
		// }
		copy(db.data[:len(bb)], bb)

		db.Close()

		db = &DB{
			path:            path,
			InitialMmapSize: 128 * 1024,
			opened:          true,
		}
		//re-open
		err = db.Open(0666)
		So(err, ShouldBeNil)
		defer db.Close()

		s = "hello xiemalin"
		v := db.data[:len(s)]
		So(s, ShouldEqual, string(v))
	})

}
