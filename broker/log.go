package broker

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

const (
	LEVELON  = 0
	LEVELOFF = 1
)

// Logger
type Logger struct {
	l     *log.Logger
	level int32
}

// SetLevel
func (l *Logger) SetLevel(level int32) {
	atomic.StoreInt32(&l.level, level)
}

// Level
func (l *Logger) Level() int32 {
	return atomic.LoadInt32(&l.level)
}

// Printf
func (l *Logger) Printf(format string, v ...interface{}) {
	if l.Level() == LEVELOFF {
		return
	}
	l.l.Output(2, fmt.Sprintf(format, v...))
}

// Print
func (l *Logger) Print(v ...interface{}) {
	if l.Level() == LEVELOFF {
		return
	}
	l.l.Output(2, fmt.Sprint(v...))
}

// Println
func (l *Logger) Println(v ...interface{}) {
	if l.Level() == LEVELOFF {
		return
	}
	l.l.Output(2, fmt.Sprintln(v...))
}

//func (l *Logger) Fatal(v ...interface{}) {
//	l.l.Output(2, fmt.Sprint(v...))
//	os.Exit(1)
//}
//
//func (l *Logger) Fatalf(format string, v ...interface{}) {
//	l.l.Output(2, fmt.Sprintf(format, v...))
//	os.Exit(1)
//}
//
//func (l *Logger) Fatalln(v ...interface{}) {
//	l.l.Output(2, fmt.Sprintln(v...))
//	os.Exit(1)
//}

// Panic
func (l *Logger) Panic(v ...interface{}) {
	s := fmt.Sprint(v...)
	l.l.Output(2, s)
	panic(s)
}

// Panicf
func (l *Logger) Panicf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	l.l.Output(2, s)
	panic(s)
}

// Panicln
func (l *Logger) Panicln(v ...interface{}) {
	s := fmt.Sprintln(v...)
	l.l.Output(2, s)
	panic(s)
}

// Flags
func (l *Logger) Flags() int {
	return l.l.Flags()
}

// SetFlags
func (l *Logger) SetFlags(flag int) {
	l.l.SetFlags(flag)
}

// Prefix
func (l *Logger) Prefix() string {
	return l.l.Prefix()
}

//SetPrefix
func (l *Logger) SetPrefix(prefix string) {
	l.l.SetPrefix(prefix)
}

var (
	Debug *Logger // Debug
	Log   *Logger // Log
	Error *Logger // Error
)

func init() {
	format := log.Ldate | log.Ltime | log.Lshortfile
	Debug = &Logger{l: log.New(os.Stdout, "[DEBUG]: ", format|log.Llongfile), level: LEVELOFF}
	Log = &Logger{l: log.New(os.Stdout, "[INFO]: ", format), level: LEVELON}
	Error = &Logger{l: log.New(os.Stderr, "[ERROR]: ", format|log.Llongfile), level: LEVELON}
}

// EnableDebug
func EnableDebug() {
	Debug.SetLevel(LEVELON)
}

// DisableDebug
func DisableDebug() {
	Debug.SetLevel(LEVELOFF)
}
