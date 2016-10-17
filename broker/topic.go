package msgo

import (
	"container/list"
	"github.com/buptmiao/msgo/msg"
	"sync"
)

type TopicQueue struct {
	broker *Broker

	topic string

	subscribeMu sync.RWMutex
	subscribes *list.List
	subscribeSet map[*subscribe]*list.Element

	buf msgChan
	stop chan struct{}
}

func NewTopicQueue(broker *Broker, topic string) *TopicQueue {
	res := new(TopicQueue)

	res.broker = broker
	res.topic = topic

	res.buf = NewMsgChan()
	res.stop = make(chan struct{})

	res.subscribes = list.New()
	res.subscribeSet = make(map[*subscribe]*list.Element)

	go res.Run()
	return res
}

func (t *TopicQueue) Run() {
	for {
		select {
		case msgs := <- t.buf:
			t.dispatch(msgs)
		case <-t.stop:
			Log.Println("topic %s closed", t.topic)
			return
		}
	}
}

func (t *TopicQueue) Bind(s *subscribe) {
	t.subscribeMu.Lock()
	defer t.subscribeMu.Unlock()
	if _, ok := t.subscribeSet[s]; ok {
		return
	}
	t.subscribes.PushBack(s)
	t.subscribeSet[s] = t.subscribes.Back()
}

func (t *TopicQueue) Unbind(s *subscribe) {
	t.subscribeMu.Lock()
	defer t.subscribeMu.Unlock()
	if v, ok := t.subscribeSet[s]; ok {
		t.subscribes.Remove(v)
		delete(t.subscribeSet, s)
	}
}

func (t *TopicQueue) Push(m *msg.Message) {
	t.buf.pushBack(m)
}

func (t *TopicQueue) dispatch(msgs []*msg.Message) {
	var send bool
	for _, m := range msgs {
		if m.GetPubType() == msg.PublishType_DirectType {
			for e := t.subscribes.Front(); e != nil; e = e.Next() {
				sub := e.Value.(*subscribe)
				if !sub.match(m) {
					continue
				}
				sub.pushMsg(m)
				t.subscribeMu.Lock()
				t.subscribes.Remove(e)
				t.subscribes.PushBack(sub)
				t.subscribeMu.Unlock()
				send = true
				break
			}
		} else {
			for e := t.subscribes.Front(); e != nil; e = e.Next() {
				sub := e.Value.(*subscribe)
				if !sub.match(m) {
					continue
				}
				sub.pushMsg(m)
				send = true
			}
		}
	}
	if !send {
		// todo delete msg
	}
}

func (t *TopicQueue) Close() {
	close(t.stop)
	close(t.buf)
}