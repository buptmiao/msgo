package main

import (
	"github.com/buptmiao/msgo/client"
	"flag"
	"github.com/buptmiao/msgo/msg"
	"fmt"
	"sync"
)

func main() {
	var addr string
	var count int
	var name string
	flag.StringVar(&addr, "addr", "localhost:13001", "the addr the message service listening on")
	flag.IntVar(&count, "count", 1, "times of consume messages")
	flag.StringVar(&name, "name", "", "name of the consumer")
	flag.Parse()
	consumer := client.NewConsumer(addr)
	wg := sync.WaitGroup{}
	wg.Add(count)
	consumer.Subscribe("msgo", "msgo", func(m ...*msg.Message) error {
		for _, v := range m {
			fmt.Println(string(v.GetBody()))
		}
		wg.Done()
		return nil
	})
	wg.Wait()
	fmt.Println(name, "exit...")
}
