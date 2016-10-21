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

//type Storage interface {
//	// it generate a auto-incrementing sequence id for m and save
//	Save(m *msg.Message) error
//
//	Get() (*msg.Message, error)
//	Delete(msgs []*msg.Message) error
//	Close() error
//	// destroy all storage info
//	Truncate()
//}

type StableStorage struct {
	db *bolt.DB
	lastkey []byte
	size int64
}

func NewStable() *StableStorage {
	res := new(StableStorage)
	var err error
	res.db, err = bolt.Open("msgo.db", 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic(err)
	}
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

func (s *StableStorage) Save(m *msg.Message) error {
	atomic.AddInt64(&s.size, 1)

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("msgo"))
		id, _ := b.NextSequence()
		m.MsgId = uint64(id)
		buf, err := m.Marshal()
		if err != nil {
			return err
		}
		return b.Put(itob(m.MsgId), buf)
	})
}

func (s *StableStorage) Get() (*msg.Message, error) {
	var res []byte
	s.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("msgo"))

		c := b.Cursor()
		k, v := c.Seek(s.lastkey)
		if bytes.Equal(k, s.lastkey) {
			k, v = c.Next()
		}
		if k == nil {
			k, v = c.First()
		}

		res = v
		s.lastkey = k
		return nil
	})
	m := &msg.Message{}

	err := m.Unmarshal(res)
	if err != nil {
		Error.Println(err)
		return nil, err
	}
	atomic.AddInt64(&s.size, -1)
	return m, nil
}

func (s *StableStorage) Delete(msgs []*msg.Message) error {
	var keys [][]byte
	for _, m := range msgs {
		if m.MsgId == 0 || !m.Persist{
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
	if err != nil {
		Error.Println(err)
	}
	return err
}

// just for test
func (s *StableStorage) Truncate() {
	filename := s.db.Path()
	s.Close()
	err := os.Remove(filename)
	if err != nil {
		panic(err)
	}
}