package broker_test

import (
	"testing"
	"github.com/buptmiao/msgo/broker"
	"github.com/buptmiao/msgo/client"
	"github.com/buptmiao/msgo/msg"
	"fmt"
	"sync"
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

	producer := client.NewProducer(addr)
	producer.PublishDirect("msgo", "msgo", []byte("hello world1"))
	producer.PublishDirectPersist("msgo", "msgo", []byte("hello world2"))
	producer.PublishFanout("msgo", "msgo", []byte("hello world3"))
	producer.PublishFanoutPersist("msgo", "msgo", []byte("hello world4"))
	//producer.PublishDirect("msgo", "msgo2", []byte("hello world5"))
	wg.Wait()
	wg.Add(1)
	fmt.Println("update")
	consumer.Subscribe("msgo", "msgo2",func(m ...*msg.Message) error {
		for _, v := range m {
			fmt.Println(string(v.GetBody()))
			wg.Done()
		}
		return nil
	})
	producer.PublishDirect("msgo", "msgo2", []byte("hello world5"))
	wg.Wait()
	err = consumer.UnSubscribe("msgo")
	if err != nil {
		panic(err)
	}
	b.Stop()
	b.Storage().Truncate()
}

func TestBroker_Delete(t *testing.T) {
	//create some date
	aof := broker.NewStorageAOF("msgo.aof", 2, 10000)
	aof.Save(newMessage())
	aof.Close()
	b := broker.GetInstance()
	b.Replay()
	b.Get("test")
	b.Delete("test")

	b.Storage().Truncate()
}