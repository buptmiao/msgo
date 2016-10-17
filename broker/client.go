package msgo

import (
	"net"
	"github.com/buptmiao/msgo/msg"
	"sync"
	"fmt"
)

type Client struct {
	sync.Mutex
	broker *Broker
	conn net.Conn
	stop chan struct{}
	subscribes map[string]*subscribe
}

func newClient(broker *Broker, conn net.Conn) *Client {
	res := new(Client)

	res.broker = broker
	res.conn = conn
	res.stop = make(chan struct{})
	res.subscribes = make(map[string]*subscribe)

	return res
}

func (c *Client) Run() {
	//
	count := 0
	for {
		// block here
		msg, err := c.recvMsg()
		if err != nil {
			break
		}
		count++
		err = c.handle(msg)
		if err != nil {
			Error.Println(err)
			c.Close()
			return
		}
	}
	Debug.Println("client %v consume %d msgs", c.conn.RemoteAddr(), count)
	c.Close()
}

// return err if it is needed to close client
func (c *Client) handle(m *msg.Message) error {
	var err error

	switch m.GetType() {
	case msg.MessageType_Subscribe:
		err = c.handleSubscribe(m)
	case msg.MessageType_Publish:
		err = c.handlePublish(m)
	case msg.MessageType_UnSubscribe:
		err = c.handleUnSubscribe(m)
	case msg.MessageType_Ack:
		err = c.handleAck(m)
	case msg.MessageType_Heartbeat:
		err = c.handleHeartbeat(m)
	default:
		panic(fmt.Errorf("unsupported msg type %v", m.GetType()))
	}
	return err
}

func (c *Client) handleSubscribe(m *msg.Message) error {
	sub, ok := c.subscribes[m.GetTopic()]
	// new subscribe
	if !ok {
		Log.Printf("%v subscribe topic %s\n", c.conn.RemoteAddr(), m.GetTopic())
		topic := c.broker.Get(m.GetTopic())
		sub = newsubscribe(topic, c, m.GetFilter(), m.GetCount(), m.GetNeedAck())
		c.subscribes[m.GetTopic()] = sub
	} else {
		sub.update(m.GetFilter(), m.GetCount())
	}

	resp := msg.NewAckMsg(m.GetTopic())

	return c.sendMsg(resp)
}

func (c *Client) handlePublish(m *msg.Message) error {

	if m.GetPersist() {
		err := c.broker.stable.Save(m)
		if err != nil {
			Error.Println(err)
			return err
		}
	} else {
		topic := c.broker.Get(m.GetTopic())
		topic.Push(m)
	}

	resp := msg.NewAckMsg(m.GetTopic())

	return c.sendMsg(resp)
}

func (c *Client) handleUnSubscribe(m *msg.Message) error {
	if sub, ok := c.subscribes[m.GetTopic()]; ok {
		Log.Printf("%v unsubscribe topic %s\n", c.conn.RemoteAddr(), m.GetTopic())
		delete(c.subscribes, m.GetTopic())
		sub.close()
	}
	// if a client subscribe no topics, then close it
	if len(c.subscribes) == 0 {
		c.Close()
	}

	resp := msg.NewAckMsg(m.GetTopic())

	return c.sendMsg(resp)
}

func (c *Client) handleAck(m *msg.Message) error {
	if sub, ok := c.subscribes[m.GetTopic()]; ok {
		sub.pushAck()
	}
	return nil
}

func (c *Client) handleHeartbeat(m *msg.Message) error {
	return nil
}

func (c *Client) recvMsg() (*msg.Message, error) {
	res, err := msg.Unmarshal(c.conn)
	if err != nil {
		Error.Println(err)
		return nil, err
	}
	return res, nil
}

func (c *Client) sendMsg(m ...*msg.Message) error {
	err := msg.BatchMarshal(&msg.MessageList{m}, c.conn)
	if err != nil {
		Error.Println(err)
	}
	return err
}

func (c *Client) Close() {
	for _, s := range c.subscribes {
		s.close()
	}
	c.subscribes = make(map[string]*subscribe)
	close(c.stop)
	err := c.conn.Close()
	if err != nil {
		Error.Println(err)
	}
	return
}