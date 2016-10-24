package client_test

import (
	"fmt"
	"github.com/buptmiao/msgo/client"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testServer(addr string, count *int64, expected int64, wait chan struct{}) net.Listener {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			if atomic.AddInt64(count, 1) == expected {
				wait <- struct{}{}
			}
			conn.Close()
		}
	}()
	return listener
}

func TestNewConnPool(t *testing.T) {
	addr := ":12345"
	pool := client.NewConnPool(100, time.Second*3, time.Second*3, addr, 100)
	if pool == nil {
		panic("create connect pool failed")
	}
}

func TestConnPoolOperation(t *testing.T) {
	addr := ":12346"
	var count int64
	wait := make(chan struct{})
	listener := testServer(addr, &count, 100, wait)
	pool := client.NewConnPool(100, time.Second*3, time.Second*3, addr, 100)
	if pool == nil {
		panic("create connect pool failed")
	}
	for i := 0; i <= 1000; i++ {
		conn, err := pool.Get()
		if conn == nil || err != nil {
			panic(fmt.Errorf("%v%v", conn, err))
		}
		go func() {
			time.Sleep(time.Millisecond)
			pool.Put(conn)
		}()
	}
	<-wait
	if atomic.LoadInt64(&count) != 100 {
		panic("unexpected conn number")
	}
	listener.Close()
}

func TestConnPool_Remove(t *testing.T) {
	addr := ":12347"
	var count int64
	wait := make(chan struct{})
	listener := testServer(addr, &count, 1000, wait)
	pool := client.NewConnPool(100, time.Second*3, time.Second*3, addr, 1000)
	if pool == nil {
		panic("create connect pool failed")
	}
	for i := 0; i < 1000; i++ {
		conn, err := pool.Get()
		if conn == nil || err != nil {
			panic(fmt.Errorf("%v%v", conn, err))
		}
		pool.Remove(conn)
	}
	//WAIT server add count
	<-wait
	if atomic.LoadInt64(&count) != 1000 {
		panic("unexpected conn number")
	}
	pool.Close()
	listener.Close()
}

func TestConnPoolConcurrency(t *testing.T) {
	addr := ":12348"
	var count int64
	wait := make(chan struct{})
	listener := testServer(addr, &count, 100, wait)
	pool := client.NewConnPool(100, time.Second*3, time.Second*3, addr, 1000)
	if pool == nil {
		panic("create connect pool failed")
	}
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				conn, err := pool.Get()
				if conn == nil || err != nil {
					panic(fmt.Errorf("%v%v", conn, err))
				}
				go func() {
					time.Sleep(time.Millisecond)
					pool.Put(conn)
				}()
			}
			wg.Done()

		}()
	}
	wg.Wait()
	<-wait
	if atomic.LoadInt64(&count) != 100 {
		panic("unexpected conn number")
	}
	listener.Close()
}
