package broker

import (
	"net"
	"sync"
)

const(
	RUNNING = iota
	STOP
)

type Broker struct {
	msg net.Listener
	http net.Listener

	stable Storage

	status int

	topicMu sync.RWMutex
	topics map[string]*TopicQueue
}

// Only one broker
var OneBroker sync.Once
var DefaultBroker *Broker

func GetInstance() *Broker {
	OneBroker.Do(NewBroker)
	return DefaultBroker
}

func NewBroker() {
	broker := new(Broker)

	var err error
	broker.msg, err = net.Listen("tcp", PortToLocalAddr(Config.MsgPort))
	PanicIfErr(err)

	broker.http, err = net.Listen("tcp", PortToLocalAddr(Config.HttpPort))
	PanicIfErr(err)

	broker.status = STOP
	broker.topics = make(map[string]*TopicQueue)

	if Config.Aof != "" {
		broker.stable = NewStorageAOF(Config.Aof, int32(Config.SyncType), Config.Threshold)
	}else{
		broker.stable = NewStable()
	}


	DefaultBroker = broker
}

func (b *Broker) Start() {
	b.status = RUNNING
	ServeHTTP(b.http)
	b.Replay()
	Log.Printf("Message service listening on port :%d\n", Config.MsgPort)
	for {
		conn, err := b.msg.Accept()
		if err != nil {
			Debug.Println(err)
			break
		}
		c := newClient(b, conn)
		go c.Run()
	}
}

func (b *Broker) Stop() {
	b.status = STOP
	if b.stable != nil {
		b.stable.Close()
	}
	if b.msg != nil {
		b.msg.Close()
	}
	if b.http != nil {
		b.http.Close()
	}
}

func (b *Broker) Get(topic string) *TopicQueue {
	b.topicMu.RLock()

	if t, ok := b.topics[topic]; ok {
		b.topicMu.RUnlock()
		return t
	} else {
		b.topicMu.RUnlock()
		t := NewTopicQueue(b, topic)
		b.topicMu.Lock()
		b.topics[topic] = t
		b.topicMu.Unlock()
		return t
	}
}

func (b *Broker) Delete(topic string) {
	b.topicMu.Lock()
	tq, ok := b.topics[topic]
	if ok {
		tq.Close()
		delete(b.topics, topic)
	}
	b.topicMu.Unlock()
}

func (b *Broker) Storage() Storage {
	return b.stable
}

// handle stable msgs
func (b *Broker) Replay() {
	for {
		if b.status == STOP {
			return
		}
		// it may block here
		m, err := b.stable.Get()
		if err != nil {
			break
		}
		topic := b.Get(m.GetTopic())
		topic.Push(m)
	}
}
