package bigqueue

import (
	"strconv"
	"strings"
	"testing"
)

func TestFanoutQueueOpen(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	fq := FileFanoutQueue{}
	err := fq.Open(path, "testqueue", nil)
	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	fq.Close()
}

func TestFanoutQueueIsEmpty(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	fanoutID := int64(100)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "testqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()

	bool := fq.IsEmpty(fanoutID)
	if !bool {
		t.Error("New created queue must be empty")
	}
}

func TestFanoutQueueSize(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	fanoutID := int64(100)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "testqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}

	defer fq.Close()

	sz := fq.Size(fanoutID)
	if sz != 0 {
		t.Error("New created queue size must be zero")
	}
}

func TestFanoutQueueEnqueue(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	fanoutID := int64(100)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "testqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()
	sz := fq.Size(fanoutID)
	if sz != 0 {
		t.Error("New created queue size must be zero")
	}

	_, err = fq.Enqueue([]byte("hello world"))

	if err != nil {
		t.Error("enqueue action failed", err)
	}

	sz = fq.Size(fanoutID)
	if sz != 1 {
		t.Error("New created queue size must be 1", sz)
	}

	bool := fq.IsEmpty(fanoutID)
	if bool {
		t.Error("New created queue must be empty", bool)
	}
}

func clearFrontIndexFiles(path, queueName string, fanoutID int64) {
	RemoveFiles(path + "/" + queueName + "/" + FanoutFrontFileName + strconv.Itoa(int(fanoutID)))
}

func TestFanoutQueueEnqueueDequeue(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID1)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "testqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()

	_, err = fq.Enqueue([]byte("hello world"))

	if err != nil {
		t.Error("enqueue action failed", err)
	}

	index, data, _ := fq.Dequeue(fanoutID)
	index1, data1, _ := fq.Dequeue(fanoutID1)

	if index != index1 {
		t.Error("index should same", index, index1)
	}

	if strings.Compare(string(data), string(data1)) != 0 {
		t.Error("data should same")
	}
}

func TestFanoutQueueEnqueuePeek(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID1)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "testqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()

	_, err = fq.Enqueue([]byte("hello world"))

	if err != nil {
		t.Error("enqueue action failed", err)
	}

	index, data, _ := fq.Peek(fanoutID)
	index1, data1, _ := fq.Peek(fanoutID1)

	if index != index1 {
		t.Error("index should same", index, index1)
	}

	if strings.Compare(string(data), string(data1)) != 0 {
		t.Error("data should same")
	}
}

func TestFanoutQueueSkip(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID)
	defer clearFrontIndexFiles(path, "testqueue", fanoutID1)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "testqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()

	for i := 0; i < 10; i++ {
		_, err = fq.Enqueue([]byte("hello world" + strconv.Itoa(i)))

		if err != nil {
			t.Error("enqueue action failed", err)
		}
	}

	fq.Skip(fanoutID, int64(5))
	index, data, _ := fq.Peek(fanoutID)
	if index != 5 {
		t.Error("index should be 5 but actually", index, data)
	}

	fq.Skip(fanoutID1, int64(1))
	index1, data1, _ := fq.Peek(fanoutID1)
	if index1 != 1 {
		t.Error("index should be 1 but actually", index1, data1)
	}
}
