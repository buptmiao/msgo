package client

import (
	"sync/atomic"
	"time"
)

// RateLimiter is based on Token Bucket algorithm, it
// supports both blocking and non-blocking way. it is
// thread-safe in concurrency
type RateLimiter struct {
	rate     int64
	remain   int64
	max      int64
	unit     int64
	lasttime int64
}

// NewRateLimiter create a new rate limiter
func NewRateLimiter(rate int64, unit time.Duration) *RateLimiter {
	res := &RateLimiter{
		rate:     rate,
		remain:   rate * int64(unit),
		max:      rate * int64(unit),
		unit:     int64(unit),
		lasttime: time.Now().UnixNano(),
	}
	return res
}

// Acquire Blocking when the limiter is available
func (r *RateLimiter) Acquire() {
	r.acquire(1, true)
}

// TryAcquire is Non-blocking, when acquire failed, it returns false.
func (r *RateLimiter) TryAcquire() bool {
	return r.acquire(1, false)
}

// AcquireCount
func (r *RateLimiter) AcquireCount(count int64) {
	if count > r.rate {
		count = r.rate
	}
	r.acquire(count, true)
}

// internal achievement
func (r *RateLimiter) acquire(count int64, block bool) bool {
	r.fill()
	var need int64
	for need = count * r.unit; atomic.LoadInt64(&r.remain) < need; r.fill() {
		if !block {
			return false
		}
		time.Sleep(time.Duration(r.unit))
	}
	atomic.AddInt64(&r.remain, -need)
	return true
}

// fill the ratelimiter
func (r *RateLimiter) fill() {
	now := time.Now().UnixNano()
	elapsed := now - atomic.SwapInt64(&r.lasttime, now)
	res := atomic.AddInt64(&r.remain, elapsed*r.rate)
	if res > r.max {
		atomic.AddInt64(&r.remain, r.max-res)
	}
}
