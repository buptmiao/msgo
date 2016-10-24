package broker_test

import (
	"github.com/buptmiao/msgo/broker"
	"testing"
)

func TestNewMsgChan(t *testing.T) {
	mc := broker.NewMsgChan()
	mc.PushBack(newMessage(), newMessage())
	mc.PushBack(newMessage())
	mc.PushFront(newMessage(), newMessage())
	mc.PushFront(newMessage(), newMessage())
	msgs := <-mc
	if len(msgs) != 7 {
		panic("msg length error")
	}
}
