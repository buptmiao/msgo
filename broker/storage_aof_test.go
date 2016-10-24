package broker_test

import (
	"github.com/buptmiao/msgo/broker"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestNewStorageAOF(t *testing.T) {
	aof := broker.NewStorageAOF("test.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	aof.Truncate()
}

func TestStorageAOF_Close(t *testing.T) {
	aof := broker.NewStorageAOF("test_close.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	aof.Close()
	aof.Truncate()
}

func TestStorageAOF_Save(t *testing.T) {
	aof := broker.NewStorageAOF("test_save.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	aof.Save(newMessage(), newMessage(), newMessage())
	aof.Truncate()
}

func TestStorageAOF_Delete(t *testing.T) {
	aof := broker.NewStorageAOF("test_delete.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	m1 := newMessage()
	m2 := newMessage()
	m3 := newMessage()
	aof.Save(m1, m2, m3)
	aof.Delete(m1, m2, m3)
	aof.Truncate()
}

func TestStorageAOF_Get(t *testing.T) {
	aof := broker.NewStorageAOF("test_get.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	m1 := newMessage()
	m2 := newMessage()
	m3 := newMessage()
	aof.Save(m1, m2, m3)
	aof.Delete(m1, m2, m3)
	aof.Rewrite(math.MaxUint64, true)

	_, err := aof.Get()
	if err != broker.ErrEmptyMsgList {
		panic(err)
	}
	aof.Truncate()
}

func TestStorageAOF_Rewrite(t *testing.T) {
	aof := broker.NewStorageAOF("test_rewrite.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	m1 := newMessage()
	m2 := newMessage()
	m3 := newMessage()
	aof.Save(m1, m2, m3)
	aof.Delete(m1, m2)
	aof.Rewrite(math.MaxUint64, true)

	m, err := aof.Get()
	if err != nil || !reflect.DeepEqual(m, m3) {
		panic(err)
	}
	aof.Truncate()
}

func TestStorageAOF_Rewrite2(t *testing.T) {
	aof := broker.NewStorageAOF("test_rewrite2.aof", 2, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	for i := 0; i < 10002; i++ {
		m1 := newMessage()
		aof.Save(m1)
		aof.Delete(m1)
	}

	for {
		if aof.DeleteOps() <= 10 {
			break
		}
		time.Sleep(time.Millisecond * 200)
	}
	inf := aof.Stat()
	if inf.Size() != 0 {
		panic("unexpected file size, not zero")
	}

	aof.Truncate()
}

func BenchmarkStorageAOF_Save(b *testing.B) {
	b.StopTimer()
	aof := broker.NewStorageAOF("benchmark_save.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		aof.Save(newMessage())
	}
	aof.Truncate()
}

func BenchmarkStorageAOF_SaveParallel(b *testing.B) {
	b.StopTimer()
	aof := broker.NewStorageAOF("benchmark_saveparallel.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			aof.Save(newMessage())
		}
	})
	aof.Truncate()
}

func BenchmarkStorageAOF_Save_NeverSync(b *testing.B) {
	b.StopTimer()
	aof := broker.NewStorageAOF("benchmark_save.aof", 0, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		aof.Save(newMessage())
	}
	aof.Truncate()
}

func BenchmarkStorageAOF_Save_EverySecond(b *testing.B) {
	b.StopTimer()
	aof := broker.NewStorageAOF("benchmark_save.aof", 1, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		aof.Save(newMessage())
	}
	aof.Truncate()
}

func BenchmarkStorageAOF_Save_AlwaysSync(b *testing.B) {
	b.StopTimer()
	aof := broker.NewStorageAOF("benchmark_save.aof", 2, 10000)
	if aof == nil {
		panic("New aof failed")
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		aof.Save(newMessage())
	}
	aof.Truncate()
}
