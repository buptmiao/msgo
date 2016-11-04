package broker_test

import (
	"errors"
	"github.com/buptmiao/msgo/broker"
	"testing"
)

func TestPanicIfErr(t *testing.T) {
	s := make(chan struct{}, 1)
	func() {
		defer EatPanic(s)
		broker.PanicIfErr(errors.New("test"))
	}()
}

func TestGetLocalIP(t *testing.T) {
	ip := broker.GetLocalIP()
	if ip == "" {
		panic("get local IP address failed")
	}
}
