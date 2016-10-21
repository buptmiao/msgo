package client

import (
	"github.com/buptmiao/msgo/msg"
	"time"
	"log"
)

type Producer struct {
	Pool *ConnPool
}

func NewProducer(addr string) *Producer {
	res := new(Producer)
	res.Pool = NewDefaultConnPool(addr)

	return res
}

//
// type:
//      0, direct; 1, fanout
func (p *Producer) publish(topic string, filter string, typ int32, body []byte, persist bool, needAck bool) error {
	if typ != 0 && typ != 1 {
		return ErrParamsInvalid
	}
	c, err := p.Pool.Get()
	if err != nil {
		return err
	}
	defer p.Pool.Put(c)
	m := &msg.Message{
		Type: msg.MessageType_Publish,
		Topic: topic,
		Filter: filter,
		Body:body,
		Timestamp: time.Now().UnixNano(),
		PubType: msg.PublishType(typ),
		Persist: persist,
		NeedAck: needAck,
	}
	err = msg.BatchMarshal(msg.PackageMsgs(m), c.c)
	if err != nil || !needAck{
		log.Println(err, 1)
		return err
	}

	return p.WaitAck(c)
}

func (p *Producer) WaitAck(c *Conn) error {
	m, err := msg.Unmarshal(c.c)
	if err != nil {
		log.Println(err, "waitack")
		return err
	}
	if m.GetType() != msg.MessageType_Ack {
		log.Println("unexpected message type", m.String())
		return ErrMsgTypeError
	}
	return nil
}

func (p *Producer) PublishDirectPersist(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 0, body, true, true)
}

func (p *Producer) PublishDirect(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 0, body, false, true)
}

func (p *Producer) PublishFanoutPersist(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 1, body, true, true)
}

func (p *Producer) PublishFanout(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 1, body, false, true)
}

// The push methods need no ack
func (p *Producer) PushDirectPersist(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 0, body, true, false)
}

func (p *Producer) PushDirect(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 0, body, false, false)
}

func (p *Producer) PushFanoutPersist(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 1, body, true, false)
}

func (p *Producer) PushFanout(topic string, filter string, body []byte) error {
	return p.publish(topic, filter, 1, body, false, false)
}

// batch publish msgs
// all the messages must have MessageType_Publish.
func (p *Producer) BatchPublish(m ...*msg.Message) error {
	for _, v := range m {
		if v.GetType() != msg.MessageType_Publish {
			return ErrMsgTypeError
		}
	}
	c, err := p.Pool.Get()
	if err != nil {
		return err
	}
	defer p.Pool.Put(c)
	return msg.BatchMarshal(msg.PackageMsgs(m...), c.c)
}

func (p *Producer) Close() {
	p.Pool.Close()
}