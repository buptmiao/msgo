package client

import (
	"errors"
)

var (
	// params invalid, please check
	ErrParamsInvalid = errors.New("invalid params")
	// msg type error
	ErrMsgTypeError = errors.New("message type error")
)
