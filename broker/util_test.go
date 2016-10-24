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
