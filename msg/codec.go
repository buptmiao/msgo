package msg

import (
	"encoding/binary"
	"io"
	"log"
)

type Proto interface {
	Size() int
	Unmarshal(data []byte) error
	MarshalTo(data []byte) (int, error)
}

func Unmarshal(r io.Reader) (*Message, error) {
	msgs, err := BatchUnmarshal(r)
	if err != nil {
		return nil, err
	}
	return msgs.Msgs[0], nil
}

func Marshal(msg *Message, w io.Writer) error {
	return BatchMarshal(PackageMsgs(msg), w)
}

func BatchUnmarshal(r io.Reader) (*MessageList, error) {
	res := new(MessageList)

	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(buf)
	buf = make([]byte, size)

	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	err := res.Unmarshal(buf)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func BatchMarshal(msgs *MessageList, w io.Writer) error {
	size := msgs.Size()
	buf := make([]byte, size+4)

	n, err := msgs.MarshalTo(buf[4:])

	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(buf[0:4], uint32(n))

	_, err = w.Write(buf[:n+4])
	if err != nil {
		log.Println(err, 2)
	}
	return err
}

func PackageMsgs(m ...*Message) *MessageList {
	res := &MessageList{}
	res.Msgs = m
	return res
}

func NewAckMsg() *Message {
	return &Message{
		Type: MessageType_Ack,
	}
}

func NewSubscribeMsg(topic string, filter string, cnt string) *Message {
	return &Message{
		Topic:  topic,
		Filter: filter,
		Type:   MessageType_Subscribe,
	}
}

func NewUnSubscribeMsg(topic string, filter string) *Message {
	return &Message{
		Topic:  topic,
		Filter: filter,
		Type:   MessageType_UnSubscribe,
	}
}

func NewPublishMsg(topic string, body []byte) *Message {
	return &Message{
		Topic: topic,
		Body:  body,
		Type:  MessageType_Publish,
	}
}

func NewHeartbeatMsg() *Message {
	return &Message{
		Type: MessageType_Heartbeat,
	}
}

func NewErrorMsg() *Message {
	return &Message{
		Type: MessageType_Error,
	}
}
