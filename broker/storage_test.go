package broker_test

import (
	"testing"
	"github.com/buptmiao/msgo/broker"
	"github.com/buptmiao/msgo/msg"
	"math/rand"
	"encoding/binary"
	"time"
	"reflect"
	"sync"
	"sync/atomic"
	"fmt"
)

func TestInitClose(t *testing.T) {
	stable := msgo.NewStable()
	stable.Truncate()
}

func randomBytes(len int) []byte{
	len = ((len + 7) >> 3) << 3
	res := make([]byte, len)
	for i := 0; i < len; i+=8 {
		s := rand.Int63()
		binary.BigEndian.PutUint64(res[i:], uint64(s))
	}
	return res
}

func randomString(len int, rang int64) string {
	len = ((len + 7) >> 3) << 3
	res := make([]byte, len)
	for i := 0; i < len; i+=8 {
		s := rand.Int63n(rang)
		binary.BigEndian.PutUint64(res[i:], uint64(s))
	}
	return string(res)
}

// pub message
func newMessage() *msg.Message {
	res := &msg.Message{}
	res.Type = msg.MessageType(rand.Int31n(int32(len(msg.MessageType_name))))

	res.Topic = randomString(16, 10)
	res.Filter = randomString(8, 3)
	res.Body = randomBytes(100)

	res.Timestamp = time.Now().UnixNano()
	res.PubType = msg.PublishType(rand.Int31n(int32(len(msg.PublishType_name))))

	res.Persist = true
	return res
}

func TestSaveAndGetMsg(t *testing.T) {
	stable := msgo.NewStable()
	m1 := newMessage()
	stable.Save(m1)
	m2,err := stable.Get()

	if err != nil || !reflect.DeepEqual(m1, m2) {
		panic("m1 != m2")
	}
	stable.Truncate()
}

func TestDeleteMsg(t *testing.T) {
	stable := msgo.NewStable()
	m1 := newMessage()
	stable.Save(m1)
	stable.Delete([]*msg.Message{m1})
	m2, _ := stable.Get()
	if reflect.DeepEqual(m1, m2) {
		panic("m1 != m2")
	}
	stable.Truncate()
}

func TestPerformance(b *testing.T) {
	stable := msgo.NewStable()
	msgs := make([]*msg.Message, 0, 10000)
	for i := 0; i < 10000; i++ {
		msgs = append(msgs, newMessage())
	}
	start := time.Now()
	index := int64(-1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	for i := 0; i < 1; i++ {
		go func() {
			for j := 0; j < 10000; j++ {
				t := atomic.AddInt64(&index, 1)
				m := msgs[t]
				stable.Save(m)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("\n10 goroutines write 10000 msgs cost %v\n", time.Since(start))

	start = time.Now()
	byteset := make([]byte, 10000)
	for i := 0; i < 10000; i++ {
		m, _ := stable.Get()
		t := m.GetMsgId() % 10000
		if byteset[t] == 1 {
			panic("data race")
		}
		byteset[t] = 1
	}
	fmt.Printf("\n1  goroutines read  10000 msgs cost %v\n", time.Since(start))

	stable.Truncate()
}