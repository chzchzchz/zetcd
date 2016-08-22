# zetcd

[![Build Status](https://travis-ci.org/chzchzchz/zetcd.svg?branch=master)](https://travis-ci.org/chzchzchz/zetcd)

A ZooKeeper "personality" for etcd. Point a ZooKeeper client at zetcd to dispatch the operations on an etcd cluster.

Protocol encoding and decoding heavily based on [go-zookeeper](http://github.com/samuel/go-zookeeper/").

## Usage

Forwarding zookeeper requests on `:2181` to an etcd server listening on `localhost:2379`,
```sh
go install github.com/chzchzchz/zetcd/cmd/zetcd
zetcd -endpoint localhost:2379  -zkaddr 0.0.0.0:2181
```

Cross-checking zookeeper emulation with a native zookeeper server on `192.168.2.1:2182`,
```sh
zetcd -endpoint localhost:2379  -zkaddr 0.0.0.0:2181 -zkbridge 192.168.2.1:2182  -oracle zk -logtostderr -v 9
```

Simple testing with `zkctl`,
```sh
go install github.com/chzchzchz/zetcd/cmd/zkctl
zkctl watch / &
zkctl put /abc "foo"
```
