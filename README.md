# msgo

[![Build Status](https://travis-ci.org/buptmiao/msgo.svg?branch=master)](https://travis-ci.org/buptmiao/msgo)
[![Coverage Status](https://coveralls.io/repos/github/buptmiao/msgo/badge.svg?branch=master)](https://coveralls.io/github/buptmiao/msgo?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/buptmiao/msgo)](https://goreportcard.com/report/github.com/buptmiao/msgo)
![License](https://img.shields.io/dub/l/vibe-d.svg)


A distributed high performance message middleware, based on pub/sub model.

## Install
```
git get -u github.com/buptmiao/msgo
```
## Features

*	Msgo uses gogoprotobuf as binary interface, gogoprotobuf is an optimized version of goprotobuf with high performance on marshaling and unmarshaling msgs and lower memory cost. For more information about gogoprotobuf, see this [benchmark](https://github.com/alecthomas/go_serialization_benchmarks) 

*   Support batch messages pub/sub, producer can publish multiple messages at a time. The broker can compose messages from different producer into a batch package, and deliver them to a subscriber at a time.

*   Support REST API, which is used to deliver messages and monitor the runtime statistics of msgo. Msgo exports the metrics as prometheus expected, that means we can collect the metrics into prometheus, and monitor the state of msgo.


## How to use

Please see the [demo](https://github.com/buptmiao/msgo/tree/master/examples/demo).


## More updates