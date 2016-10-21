package client

import (
	"sync"
	"time"
	"container/list"
	"net"
	"errors"
)

// pool state
const (
	ACTIVE = iota
	CLOSED
)

const (
	DefaultPoolSize = 10
	DefaultDialTimeout = time.Second * 5
	DefaultPoolTimeout = time.Second * 5
)

var timers = sync.Pool{
	New: func() interface{} {
		return time.NewTimer(0)
	},
}

var (
	ErrTimeout = errors.New("get conn time out")
)

type Conn struct {
	c           net.Conn
	createTime  time.Time
	totalCount  int64

	//avglifetime int64
	pool        *ConnPool
}

func newConn(c *ConnPool) (*Conn, error) {
	res := new(Conn)
	res.createTime = time.Now()
	res.totalCount = 0
	//res.avglifetime = 0
	res.pool = c
	var err error
	if res.c, err = net.DialTimeout("tcp", c.remoteAddr, c.timeout); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Conn) Close() error {
	return c.c.Close()
}

// the definition of Connect pool
//
//
//
type ConnPool struct {
	size        int
	status      int
	timeout     time.Duration
	poolTimeout time.Duration
	remoteAddr  string
	tickets     chan struct{}
	Mu          sync.Mutex
	allConn     *list.List
	idleMu      sync.Mutex
	idleConn    *list.List
}

func NewConnPool(poolSize int, dialTimeout, poolTimeout time.Duration, addr string) *ConnPool {
	p := &ConnPool{
		size:        poolSize,
		status:      ACTIVE,
		timeout:     dialTimeout,
		poolTimeout: poolTimeout,
		remoteAddr:  addr,
		tickets:     make(chan struct{}, poolSize),
		allConn:     list.New(),
		idleConn:    list.New(),
	}
	for i := 0; i < poolSize; i++ {
		p.tickets <- struct{}{}
	}
	return p
}

func NewDefaultConnPool(addr string) *ConnPool {
	return NewConnPool(DefaultPoolSize, DefaultDialTimeout, DefaultPoolTimeout, addr)
}

func (p *ConnPool) Get() (*Conn, error) {
	timer := timers.Get().(*time.Timer)
	if !timer.Reset(p.poolTimeout) {
		<-timer.C
	}
	select {
	case <-p.tickets:
		timers.Put(timer)
	case <-timer.C:
		timers.Put(timer)
		return nil, ErrTimeout
	}
	if c := p.popIdle(); c != nil {
		return c, nil
	}
	c, err := newConn(p)
	if err != nil {
		p.tickets <- struct{}{}
		return nil, err
	}
	p.push(c)
	return c, nil
}

func (p *ConnPool) popIdle() *Conn {
	p.idleMu.Lock()
	defer p.idleMu.Unlock()
	if p.idleConn.Len() <= 0 {
		return nil
	}
	c := p.idleConn.Remove(p.idleConn.Front()).(*Conn)
	return c
}

func (p *ConnPool) pushIdle(c *Conn) {
	p.idleMu.Lock()
	defer p.idleMu.Unlock()
	p.idleConn.PushBack(c)
}

func (p *ConnPool) push(c *Conn) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	p.allConn.PushBack(c)
}

func (p *ConnPool) Put(c *Conn) {
	p.pushIdle(c)
	p.tickets <- struct{}{}
}

func (p *ConnPool) Remove(c *Conn) {
	_ = c.Close()
	p.Mu.Lock()
	for e := p.allConn.Front(); e != nil; e = e.Next() {
		if e.Value.(*Conn) == c {
			p.allConn.Remove(e)
			break
		}
	}
	p.Mu.Unlock()
	p.tickets <- struct{}{}
}

func (p ConnPool) Close() {
	p.status = CLOSED
	p.Mu.Lock()
	for e := p.allConn.Front(); e != nil; e = e.Next() {
		p.allConn.Remove(e)
		e.Value.(*Conn).Close()
	}
	p.allConn = nil
	p.Mu.Unlock()
	p.idleMu.Lock()
	p.idleConn = nil
	p.idleMu.Unlock()
}
