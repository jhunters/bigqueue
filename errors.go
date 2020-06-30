package bigqueue

import "errors"

// These errors can be returned when opening or calling methods on a DB.
var (
	ErrEnqueueDataNull = errors.New("Enqueue data can not be null")

	IndexOutOfBoundTH = errors.New("Index is valid which should between tail and head index")
)
