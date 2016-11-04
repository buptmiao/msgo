package broker

import (
	"time"
	"sync"
)

//Stat instance
type Stat struct {
	startTime int64

	sync.RWMutex
	totalMsg int64
	// the msgs of per topic
	topicMsgs map[string]int64

	successMsgs int64
	successTopicMsgs map[string]int64

	// the number of subscribers of per topic
	topicSubscribers map[string]int64
}

//NewStat creates a stat object
func NewStat() *Stat{
	res := new(Stat)
	res.startTime = time.Now().UnixNano()
	res.totalMsg = 0
	res.topicMsgs = make(map[string]int64)
	res.successMsgs = 0
	res.successTopicMsgs = make(map[string]int64)
	res.topicSubscribers = make(map[string]int64)
	return res
}

//Add increase the msg number of total and topic specified
func (s *Stat) Add(topic string, num int64) {
	s.Lock()
	defer s.Unlock()
	s.totalMsg += num
	s.topicMsgs[topic] += num
}

//Success increase the msg number that has been received by consumers.
func (s *Stat) Success(topic string, num int64) {
	s.Lock()
	defer s.Unlock()
	s.successMsgs += num
	s.successTopicMsgs[topic] += num
}

func (s *Stat) Subscribe(topic string) {
	s.Lock()
	defer s.Unlock()
	s.topicSubscribers[topic] ++
}

//Get will create a replication of Stat at current time point
func (s *Stat) Get() *Stat{
	res := NewStat()
	s.RLock()
	defer s.RUnlock()
	res.startTime = s.startTime
	res.totalMsg = s.totalMsg
	for k, v := range s.topicMsgs {
		res.topicMsgs[k] = v
	}
	res.successMsgs = s.successMsgs
	for k, v := range s.successTopicMsgs {
		res.successTopicMsgs[k] = v
	}
	for k, v := range s.topicSubscribers {
		res.topicSubscribers[k] = v
	}
	return res
}
