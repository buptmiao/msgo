package client_test

import (
	"testing"
	"github.com/buptmiao/msgo/client"
	"time"
	"fmt"
	"sync"
)

func TestConcurrent(t *testing.T) {
	rl := client.NewRateLimiter(100, time.Second)
	start := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for j:=0 ; j < 10; j++ {
				rl.AcquireCount(5)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	elapse := time.Since(start)
	fmt.Println(elapse)
	if elapse < time.Second * 4 || elapse > time.Second * 6 {
		panic("rate limiter not exact 2")
	}
}

func TestNewRateLimiter(t *testing.T) {
	rl := client.NewRateLimiter(100, time.Second)
	start := time.Now()
	for i := 0; i <= 500; i++ {
		rl.Acquire()
	}
	elapse := time.Since(start)
	fmt.Println(elapse)
	if elapse < time.Second * 4 || elapse > time.Second * 6 {
		panic("rate limiter not exact 1")
	}
}

func TestRateLimiter_Acquire(t *testing.T) {
	rl := client.NewRateLimiter(10, time.Second/10)
	start := time.Now()
	for i := 0; i <= 500; i++ {
		rl.Acquire()
	}
	elapse := time.Since(start)
	fmt.Println(elapse)
	if elapse < time.Second * 4 || elapse > time.Second * 6 {
		panic("rate limiter not exact 1")
	}
}

func TestRateLimiter_AcquireCount(t *testing.T) {
	rl := client.NewRateLimiter(100, time.Second)
	start := time.Now()
	for i := 0; i <= 50; i++ {
		rl.AcquireCount(10)
	}
	elapse := time.Since(start)
	fmt.Println(elapse)
	if elapse < time.Second * 4 || elapse > time.Second * 6 {
		panic("rate limiter not exact 2")
	}
}

func TestRateLimiter_TryAcquire(t *testing.T) {
	rl := client.NewRateLimiter(100, time.Second/10)
	start := time.Now()
	for i := 0; i <= 5000; i++ {
		for !rl.TryAcquire() {
			time.Sleep(1)
		}
	}
	elapse := time.Since(start)
	fmt.Println(elapse)
	if elapse < time.Second * 4 || elapse > time.Second * 6 {
		panic("rate limiter not exact 2")
	}
}
