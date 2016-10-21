package client

import (
	"github.com/buptmiao/msgo/msg"
	"log"
	"time"
)

/////////////////////////////////////////////////////////////////////////////////////////////
// subscribe:
//		a subscribe describe a subscribe relationship with remote.
//
/////////////////////////////////////////////////////////////////////////////////////////////

type subscribe struct {
	topic  string
	filter string
	//remain msg number, if negative, it means unlimited.
	remain int64
	consumer *Consumer
	h      Handler
	last   int64
	c      *Conn
}

func (s *subscribe) run() {
	s.h.Before()
	for{
		if s.remain == 0 {
			// todo: stop this subscribe
			break
		}
		m, err := msg.BatchUnmarshal(s.c.c)
		if err != nil {
			log.Fatalln("batch unmarshal failed", err)
			break
		}

		ml := int64(len(m.Msgs))
		s.calRemain(ml)

		err = s.h.Handle(m.GetMsgs()...)
		if err != nil {
			log.Fatalln(err)  // for debug
			break
		}
		err = s.sendAck()
		if err != nil {
			log.Fatalln(err)  // for debug
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
	m, err := msg.BatchUnmarshal(s.c.c)
	if err != nil {
		return nil, err
	}
	return m.Msgs, nil
}

func (s *subscribe) sendAck() error {
	// create ack msg
	m := &msg.Message{
		Type: msg.MessageType_Ack,
		Topic: s.topic,
		Filter: s.filter,
		Timestamp: time.Now().UnixNano(),
	}
	return msg.BatchMarshal(msg.PackageMsgs(m), s.c.c)
}

func (s *subscribe) sendSub(topic string, filter string, count int64) error {
	//create subscribe msg
	m := &msg.Message{
		Type: msg.MessageType_Subscribe,
		Topic: topic,
		Filter: filter,
		Count: count,
		Timestamp: time.Now().UnixNano(),
		NeedAck: true,
	}
	return msg.BatchMarshal(msg.PackageMsgs(m), s.c.c)
}

func (s *subscribe) sendUnSub(topic string) error {
	//create subscribe msg
	m := &msg.Message{
		Type: msg.MessageType_UnSubscribe,
		Topic: topic,
		Timestamp: time.Now().UnixNano(),
		NeedAck: false,
	}
	return msg.BatchMarshal(msg.PackageMsgs(m), s.c.c)
}

func (s *subscribe) close() {
	s.c.pool.Remove(s.c)
}

type Handler interface {
	Before()
	// when Handle return value is not nil, run loop will abort, then After() will be invoked.
	Handle(...*msg.Message) error
	After()
}

// the default handler, used by Subscribe method.
type DefaultHandler struct {
	h func(...*msg.Message) error
}

func (d *DefaultHandler)Before() {
	return
}

func (d *DefaultHandler)Handle(msgs ...*msg.Message) error {
	return d.h(msgs...)
}

func (d *DefaultHandler)After() {
	return
}


/////////////////////////////////////////////////////////////////////////////////////////////
// Consumer
//
//
/////////////////////////////////////////////////////////////////////////////////////////////
type Consumer struct {
	Pool       *ConnPool
	// classify by topic
	subscribes map[string]*subscribe
}

func NewConsumer(addr string) *Consumer {
	res := new(Consumer)
	res.Pool = NewDefaultConnPool(addr)
	res.subscribes = make(map[string]*subscribe)
	return res
}
//
//
//
func (c *Consumer) subscribe(topic string, filter string, remain int64, h Handler) error {
	// this topic has been subscribed, if something changed, update it
	var sub *subscribe
	if s, ok := c.subscribes[topic]; ok {
		// nothing changed, no need to send request to broker
		if s.filter == filter {
			s.remain = remain
			s.h = h
			return nil
		}
		s.filter = filter
		s.h = h
		sub = s

		// need update
	} else {
		// create new subscribe
		s := new(subscribe)
		s.topic = topic
		s.filter = filter
		s.remain = remain
		s.h = h
		var err error
		if s.c, err = c.Pool.Get(); err != nil {
			return err
		}
		//record it
		c.subscribes[topic] = s
		sub = s
	}
	sub.sendSub(sub.topic, sub.filter, remain)
	m, err := msg.Unmarshal(sub.c.c)
	if m.Type == msg.MessageType_Ack {
		go sub.run()
	}
	return err
}

func (c *Consumer) unsubscribe(topic string) error {
	s, ok := c.subscribes[topic]
	if !ok {
		return nil
	}
	delete(c.subscribes, topic)

	// close to let remote deallocate resources.
	//
	err := s.sendUnSub(topic)
	s.close()
	return err
}

func (c *Consumer) Subscribe(topic string, filter string, f func(...*msg.Message) error) error {
	handler := &DefaultHandler{
		h: f,
	}
	return c.subscribe(topic, filter, -1, handler)
}

func (c *Consumer) SubscribeWithHandler(topic string, filter string, h Handler) error {
	return c.subscribe(topic, filter, -1, h)
}

func (c *Consumer) SubscribeWithCount(topic string, filter string, count int64, f func(...*msg.Message) error) error {
	handler := &DefaultHandler{
		h: f,
	}
	return c.subscribe(topic, filter, count, handler)
}

func (c *Consumer) SubscribeWithCountAndHandler(topic string, filter string, count int64, h Handler) error {
	return c.subscribe(topic, filter, count, h)
}

func (c *Consumer) UnSubscribe(topic string) {
	c.unsubscribe(topic)
}

func (c *Consumer) Close() {
	c.Pool.Close()
}
