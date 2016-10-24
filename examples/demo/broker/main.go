package main

import (
	"github.com/buptmiao/msgo/broker"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	broker.LoadConfig()
	//broker.EnableDebug()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	broker.GetInstance().Start()
}
