package client

type Producer struct {
	Pool *ConnPool
}

func NewProducer(addr string) *Producer {
	res := new(Producer)
	res.Pool = NewDefaultConnPool(addr)

	return res
}

func (p *Producer) Publish() {

}