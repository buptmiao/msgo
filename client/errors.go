package client

import (
	"errors"
)

var (
	//ErrParamsInvalid means params invalid, please check
	ErrParamsInvalid = errors.New("invalid params")
	//ErrMsgTypeError is msg type error
	ErrMsgTypeError = errors.New("message type error")
)
