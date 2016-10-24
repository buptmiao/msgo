package broker

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/buptmiao/msgo/msg"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	SAVE   = 1
	DELETE = 0

	NEVERSYNC   = 0
	EVERYSECOND = 1
	ALWAYSSYNC  = 2
)

var (
	ErrEmptyMsgList = errors.New("empty Msg list")
)

type StorageAOF struct {
	size        int64
	filename    string
	aof         *os.File
	rewriteflag int32

	MaxID     uint64
	deleteOps uint64
	// mutex of file and buffer operation
	aofMu sync.Mutex

	// lasttime
	syncType  int32
	threshold int

	lasttime int64

	msgs *list.List
	// rewrite, which ensure sequence of msgs
	saveBuffer    [][]byte
	deleteSet     map[uint64]struct{}
	rewriteBuffer []byte
}

func NewStorageAOF(filename string, syncType int32, threshold int) *StorageAOF {
	var err error
	res := &StorageAOF{}

	res.filename = filename
	res.syncType = syncType
	res.threshold = threshold

	res.saveBuffer = make([][]byte, 0)
	res.deleteSet = make(map[uint64]struct{})
	res.msgs = list.New()
	res.aof, err = os.OpenFile(res.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	PanicIfErr(err)
	res.Rewrite(math.MaxUint64, true)
	return res
}

func (s *StorageAOF) Save(m ...*msg.Message) error {

	bytes := s.toBinary(SAVE, m...)
	s.aofMu.Lock()
	_, err := s.aof.Write(bytes)
	if err != nil {
		Error.Println(err)
		return err
	}
	s.sync()

	// if on rewrite, then record the
	if atomic.LoadInt32(&s.rewriteflag) == 1 {
		s.rewriteBuffer = append(s.rewriteBuffer, bytes...)
	}
	s.aofMu.Unlock()
	//atomic.AddUint64(&s.saveOps, uint64(len(m)))
	if s.needRewrite() {
		go s.Rewrite(atomic.LoadUint64(&s.MaxID), false)
	}
	return nil
}

func (s *StorageAOF) Get() (*msg.Message, error) {
	if s.msgs.Len() == 0 {
		// should be ignored by application
		return nil, ErrEmptyMsgList
	}
	front := s.msgs.Front()
	res := s.msgs.Remove(front).(*msg.Message)
	return res, nil
}

func (s *StorageAOF) Delete(m ...*msg.Message) error {
	bytes := s.toBinary(DELETE, m...)
	s.aofMu.Lock()
	_, err := s.aof.Write(bytes)
	if err != nil {
		Error.Println(err)
		return err
	}
	s.sync()
	// if on rewrite, then record the
	if atomic.LoadInt32(&s.rewriteflag) == 1 {
		s.rewriteBuffer = append(s.rewriteBuffer, bytes...)
	}
	s.aofMu.Unlock()
	atomic.AddUint64(&s.deleteOps, uint64(len(m)))
	if s.needRewrite() {
		go s.Rewrite(atomic.LoadUint64(&s.MaxID), false)
	}
	return nil
}

func (s *StorageAOF) Close() error {
	err := s.aof.Close()
	s = nil
	runtime.GC()
	return err
}

func (s *StorageAOF) Truncate() {
	os.Remove(s.filename)
	s.Close()
}

// define the binary format of per cmd.
//
func (s *StorageAOF) toBinary(cmd uint8, m ...*msg.Message) []byte {
	var res []byte
	if cmd == SAVE {
		msglen := uint32(0)
		for _, v := range m {
			msglen += 1 + 4 + uint32(v.Size())
			// update the s.MaxID
			if v.GetMsgId() > atomic.LoadUint64(&s.MaxID) {
				atomic.StoreUint64(&s.MaxID, v.GetMsgId())
			}
		}
		res = make([]byte, msglen)
		offset := 0
		for _, v := range m {
			res[offset] = cmd
			size := v.Size()
			binary.BigEndian.PutUint32(res[offset+1:], uint32(size))
			v.MarshalTo(res[offset+5:])
			offset += 1 + 4 + size
		}
	} else {
		size := (1 + 8) * len(m)
		res = make([]byte, size)
		for i, v := range m {
			res[i*9] = DELETE
			binary.BigEndian.PutUint64(res[i*9+1:], v.GetMsgId())
		}
	}
	return res
}

// With param  curMaxID, it won't rewrite messages that writen into aof
// file after rewrite started. I achieve this by compare the MaxID, when
// background rewrite is invoked, pass  s.MaxID as params curMaxID. when
// discover a Message ID > curMaxID in the progress of reading old file,
// then break. however, messages should never be duplicated. this takes
// effect on SAVE cmd. and DELETE cmd won't need de-duplicated, because
// msgID is never duplicated, and each can only be delete once.
func (s *StorageAOF) Rewrite(curMaxID uint64, startup bool) bool {
	if !atomic.CompareAndSwapInt32(&s.rewriteflag, 0, 1) {
		return false
	}
	if !startup {
		Log.Println("aof rewrite started...")
	}
	oldfile, err := os.Open(s.filename)
	PanicIfErr(err)
	tmpfilename := fmt.Sprintf("tmp_%s", s.filename)
	os.Remove(tmpfilename)
	newfile, err := os.OpenFile(tmpfilename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	PanicIfErr(err)
	s.load(oldfile)
	deleteOps := s.store(curMaxID, newfile, startup)
	s.aofMu.Lock()
	newfile.Write(s.rewriteBuffer)
	newfile.Sync()
	if !startup {
		s.resetRewrite(deleteOps)
	}
	//old
	s.aof = newfile
	err = os.Rename(tmpfilename, s.filename)
	PanicIfErr(err)
	s.aofMu.Unlock()
	oldfile.Close()
	// set rewrite flag to zero
	atomic.StoreInt32(&s.rewriteflag, 0)
	return true
}

// load will load the old file into s.saveBuffer and s.deleteSet
//
func (s *StorageAOF) load(old *os.File) {
	bytes := make([]byte, 9)
	for {
		_, err := old.Read(bytes)
		// EOF or another error
		if err != nil {
			//Log.Println("read over", err)
			break
		}
		if bytes[0] == SAVE {
			size := binary.BigEndian.Uint32(bytes[1:])
			msg := make([]byte, size)
			copy(msg, bytes[5:])
			_, err := old.Read(msg[4:])
			// impossible unless file is broken
			PanicIfErr(err)
			s.saveBuffer = append(s.saveBuffer, msg)
		} else if bytes[0] == DELETE {
			msgID := binary.BigEndian.Uint64(bytes[1:])
			s.deleteSet[msgID] = struct{}{}
		} else {
			panic("file format error")
		}
	}
}

// store will write the diff of s.saveBuffer and s.deleteSet into newfile
// the return value of store is the items that have been deleted, it is used
// to update s.deleteOps by s.resetRewrite()
func (s *StorageAOF) store(curMaxID uint64, newfile *os.File, startup bool) uint64 {
	var res uint64
	for _, v := range s.saveBuffer {
		msg := &msg.Message{}
		err := msg.Unmarshal(v)
		PanicIfErr(err)

		// to avoid duplicate messages with rewriteBuffer.
		if msg.GetMsgId() > curMaxID {
			break
		}
		if _, ok := s.deleteSet[msg.MsgId]; ok {
			delete(s.deleteSet, msg.MsgId)
			res++
			continue
		}
		// when init, push the msg to s.msgs for recover the msgs
		if startup {
			s.msgs.PushBack(msg)
		}
		// recover the maxID
		if msg.MsgId > s.MaxID {
			s.MaxID = msg.MsgId
		}
		bytes := make([]byte, 1+4+len(v))
		bytes[0] = SAVE
		binary.BigEndian.PutUint32(bytes[1:], uint32(len(v)))
		copy(bytes[5:], v)
		newfile.Write(bytes)
	}
	newfile.Sync()
	return res
}

// Sync the aof file by different strategies
//	Never sync, every second and always
func (s *StorageAOF) sync() {
	switch s.syncType {
	case NEVERSYNC:
		return
	case EVERYSECOND:
		if time.Now().UnixNano()-s.lasttime > int64(time.Second) {
			s.aof.Sync()
			s.lasttime = time.Now().UnixNano()
		}
	case ALWAYSSYNC:
		s.aof.Sync()
		s.lasttime = time.Now().UnixNano()
	}
}

// It returns true only if deleteops exceed threshold and no background rewrite goroutine.
func (s *StorageAOF) needRewrite() bool {
	if s.DeleteOps() > uint64(s.threshold) && atomic.LoadInt32(&s.rewriteflag) == 0 {
		return true
	}
	return false
}

// reset rewrite related info
func (s *StorageAOF) resetRewrite(deleteOps uint64) {
	s.saveBuffer = nil
	s.deleteSet = make(map[uint64]struct{})
	s.rewriteBuffer = nil
	atomic.AddUint64(&s.deleteOps, -deleteOps)
}

func (s *StorageAOF) DeleteOps() uint64 {
	return atomic.LoadUint64(&s.deleteOps)
}

func (s *StorageAOF) Stat() os.FileInfo {
	inf, _ := s.aof.Stat()
	return inf
}
