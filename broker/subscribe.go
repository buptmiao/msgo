package msgo

import (
	"github.com/buptmiao/msgo/msg"
	"time"
)

//
type msgChan chan []*msg.Message

func NewMsgChan() msgChan {
	return make(chan []*msg.Message, 1)
}

func (pc msgChan) pushBack(cmd ...*msg.Message) {
	toStack := cmd
	for {
		select {
		case pc <- toStack:
			return
		case old := <-pc:
			toStack = append(old, toStack...)
		}
	}
}

func (pc msgChan) pushFront(cmd ...*msg.Message) {
	toStack := cmd
	for {
		select {
		case pc <- toStack:
			return
		case old := <-pc:
			toStack = append(toStack, old...)
		}
	}
}

// A channel is defined as a subscribe relationship.
type subscribe struct {
	topic *TopicQueue
	client *Client
	filter string
	buf msgChan

	NeedAck bool
	ack chan struct{}
	wait bool

	remain int64
}

func newsubscribe(topic *TopicQueue, client *Client, filter string, cnt int64, ack bool) *subscribe {
	res := new(subscribe)

	res.topic = topic
	res.filter = filter
	res.buf = NewMsgChan()
	res.remain = cnt

	res.NeedAck = ack
	res.ack = make(chan struct{}, Config.Retry)

	topic.Bind(res)
	go res.Run()
	return res
}

func (s *subscribe) Run() {
	for {
		select {
		case msgs := <- s.buf:
			s.sendMsg(msgs)
		case <-s.ack:
			if !s.NeedAck {
				Error.Println("unexpected ack")
			}
			//ignore
			Error.Println("unexpected ack, ignore it")
		case <-s.client.stop:
			return
		}
	}
}

func (s *subscribe) sendMsg(msgs []*msg.Message) {
	s.ack = make(chan struct{}, Config.Retry)
	for i := 0; i < Config.Retry; i ++ {
		s.client.sendMsg(msgs...)

		if s.NeedAck {
			select {
			case <-s.ack:
				// todo delete msg.
				return
			case <-time.After(time.Second * 5):
				Error.Printf("time out! client:%v no ack to %s, time %d \n", s.client.conn.RemoteAddr(), s.topic.topic, i+1)
			}
		}
	}
}

func (s *subscribe) match(m *msg.Message) bool {
	if s.filter == m.GetFilter() {
		return true
	}
	return false
}

func (s *subscribe) pushMsg(m *msg.Message) {
	s.buf.pushBack(m)
}

func (s *subscribe) pushAck() {
	s.ack <- struct {}{}
}

func (s *subscribe) update(filter string, cnt int64) {
	s.filter = filter
	s.remain = cnt
}

func (s *subscribe) close() {
	close(s.buf)
}