package main

import (
	"flag"
	"github.com/buptmiao/msgo/client"
	"log"
)

func main() {
	var addr string
	flag.StringVar(&addr, "addr", "localhost:13001", "the addr the message service listening on")
	flag.Parse()
	producer := client.NewProducer(addr)
	err := producer.PublishFanout("msgo", "msgo", []byte("hello world"))
	if err != nil {
		log.Fatalln(err)
	}
	producer.Close()
}
