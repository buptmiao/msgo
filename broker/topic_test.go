package broker_test

import (
	"github.com/buptmiao/msgo/broker"
	"testing"
	"time"
)

func TestTopicQueue_Close(t *testing.T) {
	bro := broker.GetInstance()
	tq := broker.NewTopicQueue(bro, "topic")
	for {
		if tq.Status() == 0 {
			break
		}
		time.Sleep(time.Millisecond * 20)
	}
	// fake
	tq.Bind(nil)
	tq.Close()
	for {
		if tq.Status() == 1 {
			break
		}
		time.Sleep(time.Millisecond * 20)
	}
}
