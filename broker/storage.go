package broker

import (
	"time"

	"github.com/buptmiao/msgo/msg"
	"github.com/boltdb/bolt"
	"fmt"
	"sync/atomic"
	"bytes"
	"os"
)

type Storage interface {
	Save(m ...*msg.Message) error
	Get() (*msg.Message, error)
	Delete(m ...*msg.Message) error
	Close() error
	Truncate()
}

type StableStorage struct {
	db      *bolt.DB
	lastkey []byte
	size    int64
}

func NewStable() *StableStorage {
	res := new(StableStorage)
	var err error
	res.db, err = bolt.Open("msgo.db", 0666, &bolt.Options{Timeout: 1 * time.Second})
	PanicIfErr(err)
	//must initiate bucket msgo
	res.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("msgo"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		// size of msgs
		res.size = int64(b.Stats().KeyN)
		return nil
	})
	res.lastkey = nil

	return res
}

func (s *StableStorage) Save(m ...*msg.Message) error {
	atomic.AddInt64(&s.size, 1)

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("msgo"))
		for _, v := range m {
			id, _ := b.NextSequence()
			v.MsgId = uint64(id)
			buf, err := v.Marshal()
			if err != nil {
				return err
			}
			err = b.Put(itob(v.MsgId), buf)
			return err
		}
		return nil
	})
}

func (s *StableStorage) Get() (*msg.Message, error) {
	var res []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("msgo"))

		c := b.Cursor()
		k, v := c.Seek(s.lastkey)
		if bytes.Equal(k, s.lastkey) {
			k, v = c.Next()
		}
		if k == nil {
			return ErrEmptyMsgList
		}

		res = v
		s.lastkey = k
		return nil
	})
	if err != nil {
		return nil, err
	}
	m := &msg.Message{}

	err = m.Unmarshal(res)
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&s.size, -1)
	return m, nil
}

func (s *StableStorage) Delete(msgs ...*msg.Message) error {
	var keys [][]byte
	for _, m := range msgs {
		if m.MsgId == 0 || !m.Persist {
			continue
		}
		keys = append(keys, itob(m.MsgId))
	}
	return s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("msgo"))

		for _, v := range keys {
			err := b.Delete(v)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *StableStorage) Close() error {
	err := s.db.Close()
	PanicIfErr(err)
	return err
}

// just for test
func (s *StableStorage) Truncate() {
	filename := s.db.Path()
	s.Close()
	err := os.Remove(filename)
	PanicIfErr(err)
}