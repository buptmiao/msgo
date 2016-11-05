# msgo

[![Build Status](https://travis-ci.org/buptmiao/msgo.svg?branch=master)](https://travis-ci.org/buptmiao/msgo)
[![Coverage Status](https://coveralls.io/repos/github/buptmiao/msgo/badge.svg?branch=master)](https://coveralls.io/github/buptmiao/msgo?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/buptmiao/msgo)](https://goreportcard.com/report/github.com/buptmiao/msgo)
[![Docker Repository on Quay](https://quay.io/repository/buptmiao/msgo/status "Docker Repository on Quay")](https://quay.io/repository/buptmiao/msgo)


A distributed high performance message middleware, based on pub/sub model.

## Install
```
git get -u github.com/buptmiao/msgo
```
## Features

*	Msgo uses gogoprotobuf as binary interface, gogoprotobuf is an optimized version of goprotobuf with high performance on marshaling and unmarshaling msgs and lower memory cost. For more information about gogoprotobuf, see this [benchmark](https://github.com/alecthomas/go_serialization_benchmarks) 

*   Support batch messages pub/sub, producer can publish multiple messages at a time. The broker can compose messages from different producer into a batch package, and deliver them to a subscriber at a time.

*   Support REST API, which is used to deliver messages and monitor the runtime statistics of msgo. Msgo exports the metrics as [prometheus](https://prometheus.io/) expected, that means we can collect the metrics into [prometheus](https://prometheus.io/), and monitor the state of msgo.

*   Msgo supports two kind of message type: persist and non-persist, with non-persist type, messages are stored in memory, and with persist type, messages are stored into disk. Msgo supports two kinds of persistence strategy, [Boltdb](https://github.com/boltdb/bolt) and customized AOF storage which is inspired by redis aof.
 
## How to use

#### subscribe a topic with a filter
```go
consumer := client.NewConsumer(addr)  //addr is the address of broker, eg: localhost:13001
consumer.Subscribe("msgo", "filter", func(m ...*msg.Message) error {
    for _, v := range m {
        fmt.Println(string(v.GetBody()))
    }
    return nil
})
```

#### publish a message

```go
producer := client.NewProducer(addr)
_ = producer.PublishFanout("msgo", "filter", []byte("hello world"))

```

then the subscriber will received a message "hello world".


Please see the [demo](https://github.com/buptmiao/msgo/tree/master/examples/demo) for details.

## Docker
```
docker pull buptmiao/msgo
docker run --name broker -p 13000:13000 -p 13001:13001 buptmiao/msgo
```

## More updates