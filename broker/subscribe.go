package broker

import (
	"github.com/buptmiao/msgo/msg"
	"time"
)

//
type msgChan chan []*msg.Message

//NewMsgChan creates a new msgChan
func NewMsgChan() msgChan {
	return make(chan []*msg.Message, 1)
}

//PushBack
func (pc msgChan) PushBack(cmd ...*msg.Message) {
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

//PushFront
func (pc msgChan) PushFront(cmd ...*msg.Message) {
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

//A channel is defined as a subscribe relationship.
type subscribe struct {
	topic  *TopicQueue
	client *Client
	filter string
	buf    msgChan

	NeedAck bool
	ack     chan struct{}
	wait    bool

	remain int64
}

func newsubscribe(topic *TopicQueue, client *Client, filter string, cnt int64, ack bool) *subscribe {
	res := new(subscribe)

	res.topic = topic
	res.filter = filter
	res.buf = NewMsgChan()
	res.remain = cnt

	res.client = client
	res.NeedAck = ack
	res.ack = make(chan struct{}, Config.Retry)

	topic.Bind(res)
	go res.Run()
	return res
}

//Run loop
func (s *subscribe) Run() {
	for {
		select {

		case msgs := <-s.buf:
			//may be closed
			if len(msgs) == 0 {
				continue
			}
			s.sendMsg(msgs)
		case <-s.ack:
			if !s.NeedAck {
				Error.Println("unexpected ack")
			}
			//ignore
			Error.Println("unexpected ack, ignore it", s)
		case <-s.client.stop:
			Debug.Println("subscribe stop by client")
			return
		}
	}
}

func (s *subscribe) sendMsg(msgs []*msg.Message) {
	s.ack = make(chan struct{}, Config.Retry)
	for i := 0; i < Config.Retry+1; i++ {
		s.client.sendMsg(msgs...)

		if s.NeedAck {
			select {
			case <-s.ack:
				//todo delete msg.
				Debug.Println("recv ack")
				return
			case <-time.After(time.Second * 5):
				Error.Printf("time out! client:%v no ack to %s, time %d \n", s.client.conn.RemoteAddr(), s.topic.topic, i+1)
			}
		} else {
			return
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
	s.buf.PushBack(m)
}

func (s *subscribe) pushAck() {
	s.ack <- struct{}{}
}

func (s *subscribe) update(filter string, cnt int64) {
	s.filter = filter
	s.remain = cnt
}

func (s *subscribe) close() {
	s.topic.Unbind(s)
	close(s.buf)
}
