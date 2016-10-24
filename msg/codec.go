package msg

import (
	"encoding/binary"
	"io"
	"log"
)

//Unmarshal the msg
func Unmarshal(r io.Reader) (*Message, error) {
	msgs, err := BatchUnmarshal(r)
	if err != nil {
		return nil, err
	}
	return msgs.Msgs[0], nil
}

//Marshal the msg
func Marshal(msg *Message, w io.Writer) error {
	return BatchMarshal(PackageMsgs(msg), w)
}

//BatchUnmarshal the msgs
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

//BatchMarshal the msgs
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

//PackageMsgs msgs
func PackageMsgs(m ...*Message) *MessageList {
	res := &MessageList{}
	res.Msgs = m
	return res
}

//NewAckMsg creates a Ack msg
func NewAckMsg() *Message {
	return &Message{
		Type: MessageType_Ack,
	}
}

//NewSubscribeMsg creates a subscribe msg
func NewSubscribeMsg(topic string, filter string, cnt string) *Message {
	return &Message{
		Topic:  topic,
		Filter: filter,
		Type:   MessageType_Subscribe,
	}
}

//NewUnSubscribeMsg creates a unSubscribe msg
func NewUnSubscribeMsg(topic string, filter string) *Message {
	return &Message{
		Topic:  topic,
		Filter: filter,
		Type:   MessageType_UnSubscribe,
	}
}

//NewPublishMsg creates a publish msg
func NewPublishMsg(topic string, body []byte) *Message {
	return &Message{
		Topic: topic,
		Body:  body,
		Type:  MessageType_Publish,
	}
}

//NewHeartbeatMsg creates a heartbeat msg
func NewHeartbeatMsg() *Message {
	return &Message{
		Type: MessageType_Heartbeat,
	}
}

//NewErrorMsg creates a error msg
func NewErrorMsg() *Message {
	return &Message{
		Type: MessageType_Error,
	}
}
