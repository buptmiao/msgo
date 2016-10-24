package broker_test

import (
	"testing"
	"errors"
	"github.com/buptmiao/msgo/broker"
)



func TestPanicIfErr(t *testing.T) {
	s := make(chan struct{}, 1)
	func() {
		defer EatPanic(s)
		broker.PanicIfErr(errors.New("test"))
	}()
}