package broker_test

import (
	"fmt"
	"github.com/buptmiao/msgo/broker"
	"github.com/buptmiao/msgo/client"
	"github.com/buptmiao/msgo/msg"
	"sync"
	"testing"
	"time"
)

func TestSubscribeAndPublish(t *testing.T) {
	loadConfig()
	broker.EnableDebug()
	b := broker.GetInstance()
	go b.Start()
	addr := fmt.Sprintf("127.0.0.1:%d", broker.Config.MsgPort)
	consumer := client.NewConsumer(addr)
	wg := sync.WaitGroup{}
	wg.Add(4)
	err := consumer.Subscribe("msgo", "msgo", func(m ...*msg.Message) error {
		for _, v := range m {
			fmt.Println(string(v.GetBody()))
			wg.Done()
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = consumer.Subscribe("delay", "msgo", func(m ...*msg.Message) error {
		for _, v := range m {
			fmt.Println(string(v.GetBody()))
		}
		time.Sleep(time.Second * 6)
		return nil
	})
	if err != nil {
		panic(err)
	}
	producer := client.NewProducer(addr)
	producer.PublishDirect("msgo", "msgo", []byte("hello world1"))
	producer.PublishDirectPersist("msgo", "msgo", []byte("hello world2"))
	producer.PublishFanout("msgo", "msgo", []byte("hello world3"))
	producer.PublishFanoutPersist("msgo", "msgo", []byte("hello world4"))
	//producer.PublishDirect("msgo", "msgo2", []byte("hello world5"))
	wg.Wait()
	wg.Add(1)
	fmt.Println("update")
	consumer.Subscribe("msgo", "msgo2", func(m ...*msg.Message) error {
		for _, v := range m {
			fmt.Println(string(v.GetBody()))
			wg.Done()
		}
		return nil
	})
	producer.PublishDirect("msgo", "msgo2", []byte("hello world5"))
	producer.PublishDirect("delay", "msgo", []byte("wait"))
	wg.Wait()
	err = consumer.UnSubscribe("msgo")
	if err != nil {
		panic(err)
	}
	//wait to test delay
	time.Sleep(time.Second * 7)
	consumer.Close()
	b.Stop()
	b.Storage().Truncate()
}

func TestBroker_GetAndDelete(t *testing.T) {
	//use stable
	db := broker.NewStable()
	db.Save(newMessage())
	db.Close()
	tmp := broker.Config.Aof
	broker.Config.Aof = ""

	broker.OneBroker = sync.Once{}
	b := broker.GetInstance()
	b.Replay()
	tq := b.Get("test")
	if tq == nil {
		panic("")
	}
	b.Delete("test")

	if b.TotalTopic() != 1 {
		panic("expected 1")
	}
	b.Storage().Truncate()
	broker.Config.Aof = tmp
}
