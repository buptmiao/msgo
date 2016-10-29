## Demo

This is a demo usage of msgo

Please install [goreman](https://github.com/mattn/goreman) for convenience, which is not necessary.
goreman is an applications management tool based on Procfile.

#### Get goreman

```
go get -u github.com/mattn/goreman
```

#### Run this cmd to build our binaries.
```
goreman start build
```

this cmd will generate three binaries: brokercmd, consumercmd and producercmd. 

#### Launch broker
```
goreman start broker
```

Now, start up two consumers and two producers, each producer will publish a message to both consumers, and consumers will exit when they receive two messages.

#### Start up two consumers in another shell window
```
goreman start consumer1 consumer2
```


#### Start up two producers in another shell window
```
goreman start producer1 producer2
```

Look at the output of consumers:

```
16:19:47 consumer2 | hello world
16:19:47 consumer1 | hello world
16:19:47 consumer1 | hello world
16:19:47 consumer1 | consumer1 exit...
16:19:47 consumer2 | hello world
16:19:47 consumer2 | consumer2 exit...
16:19:47 consumer1 | Terminating consumer1
16:19:47 consumer2 | Terminating consumer2
```
