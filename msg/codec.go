package msg

import (
	"io"
	"encoding/binary"
	"log"
)

type Proto interface {
	Size() int
	Unmarshal(data []byte) error
	MarshalTo(data []byte) (int, error)
}

func Unmarshal(r io.Reader) (*Message, error) {
	res := new(Message)

	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		log.Fatalln(err)
		return nil, err
	}

	size := binary.BigEndian.Uint32(buf)
	buf = make([]byte, size)

	if _, err := io.ReadFull(r, buf); err != nil {
		log.Fatalln(err)
		return nil, err
	}

	err := res.Unmarshal(buf)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	return res, nil
}

func Marshal(msg *Message, w io.Writer) error {
	size := msg.Size()
	buf := make([]byte, size + 4)

	n, err := msg.MarshalTo(buf[4:])

	if err != nil {
		log.Fatalln(err)
		return err
	}
	binary.BigEndian.PutUint32(buf[0:4], uint32(n))

	_, err = w.Write(buf[:n+4])
	if err != nil {
		log.Fatalln(err)
	}
	return err
}

func BatchUnmarshal(r io.Reader) (*MessageList, error) {
	res := new(MessageList)

	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		log.Fatalln(err)
		return nil, err
	}

	size := binary.BigEndian.Uint32(buf)
	buf = make([]byte, size)

	if _, err := io.ReadFull(r, buf); err != nil {
		log.Fatalln(err)
		return nil, err
	}

	err := res.Unmarshal(buf)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	return res, nil
}

func BatchMarshal(msgs *MessageList, w io.Writer) error {
	size := msgs.Size()
	buf := make([]byte, size + 4)

	n, err := msgs.MarshalTo(buf[4:])

	if err != nil {
		log.Fatalln(err)
		return err
	}
	binary.BigEndian.PutUint32(buf[0:4], uint32(n))

	_, err = w.Write(buf[:n+4])
	if err != nil {
		log.Fatalln(err)
	}
	return err
}



func NewAckMsg(topic string) *Message {
	return &Message{
		Topic : topic,
		Type  : MessageType_Ack,
	}
}

func NewSubscribeMsg(topic string, filter string, cnt string) *Message {
	return &Message{
		Topic : topic,
		Filter: filter,
		Type  : MessageType_Subscribe,
	}
}

func NewUnSubscribeMsg(topic string, filter string) *Message {
	return &Message{
		Topic : topic,
		Filter: filter,
		Type  : MessageType_UnSubscribe,
	}
}

func NewPublishMsg(topic string, body []byte) *Message {
	return &Message{
		Topic : topic,
		Body: body,
		Type  : MessageType_Publish,
	}
}

func NewHeartbeatMsg() *Message {
	return &Message{
		Type  : MessageType_Heartbeat,
	}
}

func NewErrorMsg() *Message {
	return &Message{
		Type  : MessageType_Error,
	}
}