package client

import(
	"errors"
)

var (
	ErrParamsInvalid = errors.New("invalid params")
	ErrMsgTypeError = errors.New("message type error")
)

