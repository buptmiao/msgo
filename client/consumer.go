package client

import (
	"github.com/buptmiao/msgo/msg"
	"github.com/prometheus/common/log"
	"time"
)

type subscribe struct {
	topic  string
	filter string
	//remain msg number, if negative, it means unlimited.
	remain int64

	h      Handler
	last   int64
	c      *Conn
}

func (s *subscribe) run() {
	s.h.Before()
	for{
		if s.remain == 0 {
			// todo: stop this subscribe
			_ = s.close()
			break
		}
		m, err := msg.BatchUnmarshal(s.c)
		if err != nil {
			log.Fatalln("batch unmarshal failed", err)
			break
		}

		ml := int64(len(m.Msgs))
		s.calRemain(ml)

		err = s.h.Handle(m.GetMsgs()...)
		if err != nil {
			log.Fatalln(err)
			break
		}
	}
	s.h.After()
}

func (s *subscribe) calRemain(v int64) {
	// means forever
	if s.remain < 0 {
		return
	}
	s.remain -= v
	if s.remain < 0 {
		s.remain = 0
	}
}

func (s *subscribe) recvMsgs() ([]*msg.Message, error) {
	m, err := msg.BatchUnmarshal(s.c)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *subscribe) sendAck() error {
	// create ack msg
	m := &msg.Message{
		Type: msg.MessageType_Ack,
		Topic: s.topic,
		Filter: s.filter,
		Timestamp: time.Now().UnixNano(),
	}
	return msg.Marshal(m, s.c)
}

func (s *subscribe) sendSub(topic string, filter string) error {
	//create subscribe msg
	m := &msg.Message{
		Type: msg.MessageType_Subscribe,
		Topic: topic,
		Filter: filter,
		Timestamp: time.Now().UnixNano(),
		NeedAck: true,
	}
	return msg.Marshal(m, s.c)
}

func (s *subscribe) sendUnSub(topic string, filter string) error {
	//create subscribe msg
	m := &msg.Message{
		Type: msg.MessageType_UnSubscribe,
		Topic: topic,
		Filter: filter,
		Timestamp: time.Now().UnixNano(),
		NeedAck: true,
	}
	return msg.Marshal(m, s.c)
}

func (s *subscribe) close() error {
	return s.c.Close()
}

type Handler interface {
	Before()
	// when Handle return value is not nil, run loop will abort, then After() will be invoked.
	Handle(...*msg.Message) error
	After()
}

type Consumer struct {
	Pool       *ConnPool
	subscribes map[string]*subscribe
}

func NewConsumer(addr string) *Consumer {
	res := new(Consumer)
	res.Pool = NewDefaultConnPool(addr)

	return res
}

//
//
//
//
func (c *Consumer) subscribe(topic string, filter string, remain int64, h Handler) error {
	// this topic has been subscribed, if something changed, update it
	if s, ok := c.subscribes[topic]; ok {
		// nothing changed, no need to send request to broker
		if s.filter == filter {
			s.remain = remain
			s.h = h
			return nil
		}
		// need update
	} else {
		// create new subscribe
		s := new(subscribe)
		s.topic = topic
		s.filter = filter
		s.remain = remain
		var err error
		if s.c, err = c.Pool.Get(); err != nil {
			return err
		}
		//record it
		c.subscribes[topic] = s
	}

	return nil
}

func (c *Consumer) unsubscribe(topic string) error {
	s, ok := c.subscribes[topic]
	if !ok {
		return nil
	}
	delete(c.subscribes, s)
	// close to let remote deallocate resources.
	return s.close()
}

























