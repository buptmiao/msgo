package broker

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

const (
	//LEVELON status
	LEVELON = 0
	//LEVELOFF status
	LEVELOFF = 1
)

//Logger instance
type Logger struct {
	l     *log.Logger
	level int32
}

//SetLevel set switch on/off
func (l *Logger) SetLevel(level int32) {
	atomic.StoreInt32(&l.level, level)
}

//Level shows level
func (l *Logger) Level() int32 {
	return atomic.LoadInt32(&l.level)
}

//Printf wrapper the log.Printf
func (l *Logger) Printf(format string, v ...interface{}) {
	if l.Level() == LEVELOFF {
		return
	}
	l.l.Output(2, fmt.Sprintf(format, v...))
}

//Print wrapper the log.Print
func (l *Logger) Print(v ...interface{}) {
	if l.Level() == LEVELOFF {
		return
	}
	l.l.Output(2, fmt.Sprint(v...))
}

//Println wrapper the log.Println
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

//Panic wrapper the log.Panic
func (l *Logger) Panic(v ...interface{}) {
	s := fmt.Sprint(v...)
	l.l.Output(2, s)
	panic(s)
}

//Panicf wrapper the log.Panicf
func (l *Logger) Panicf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	l.l.Output(2, s)
	panic(s)
}

//Panicln wrapper the log.Panicln
func (l *Logger) Panicln(v ...interface{}) {
	s := fmt.Sprintln(v...)
	l.l.Output(2, s)
	panic(s)
}

//Flags wrapper the log.Flags
func (l *Logger) Flags() int {
	return l.l.Flags()
}

//SetFlags wrapper the log.SetFlags
func (l *Logger) SetFlags(flag int) {
	l.l.SetFlags(flag)
}

//Prefix wrapper the log.Prefix
func (l *Logger) Prefix() string {
	return l.l.Prefix()
}

//SetPrefix wrapper the log.SetPrefix
func (l *Logger) SetPrefix(prefix string) {
	l.l.SetPrefix(prefix)
}

var (
	//Debug level
	Debug *Logger
	//Log level
	Log *Logger
	//Error level
	Error *Logger
)

func init() {
	format := log.Ldate | log.Ltime | log.Lshortfile
	Debug = &Logger{l: log.New(os.Stdout, "[DEBUG]: ", format|log.Llongfile), level: LEVELOFF}
	Log = &Logger{l: log.New(os.Stdout, "[INFO]: ", format), level: LEVELON}
	Error = &Logger{l: log.New(os.Stderr, "[ERROR]: ", format|log.Llongfile), level: LEVELON}
}

//EnableDebug enable Debug logs
func EnableDebug() {
	Debug.SetLevel(LEVELON)
}

//DisableDebug disable Debug logs
func DisableDebug() {
	Debug.SetLevel(LEVELOFF)
}
