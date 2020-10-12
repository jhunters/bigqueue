package bigqueue

import "errors"

// These errors can be returned when opening or calling methods on a DB.
var (
	ErrEnqueueDataNull = errors.New("enqueue data can not be null")

	ErrIndexOutOfBoundTH = errors.New("index is valid which should between tail and head index")

	// SubscribeExistErr repeat call Subscriber method
	ErrSubscribeExistErr = errors.New("Subscriber alread set, can not repeat set")

	// Subscribe should call after queue Open method
	ErrSubscribeFailedNoOpenErr = errors.New("Subscriber method only support after queue opened")
)
