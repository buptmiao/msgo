package main

import (
	"github.com/buptmiao/msgo/broker"
	_ "net/http/pprof"
	"log"
	"net/http"
)

func main() {
	broker.LoadConfig()
	//broker.EnableDebug()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	broker.GetInstance().Start()
}
