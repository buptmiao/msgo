package broker_test

import (
	"testing"
	"github.com/buptmiao/msgo/broker"
)

func TestTopicQueue_Close(t *testing.T) {
	bro := broker.GetInstance()
	tq := broker.NewTopicQueue(bro, "topic")
	tq.Close()
}
