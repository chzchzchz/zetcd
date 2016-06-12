# zetcd

[![Build Status](https://travis-ci.org/chzchzchz/zetcd.svg?branch=master)](https://travis-ci.org/chzchzchz/zetcd)

A ZooKeeper "personality" for etcd.
Point a ZooKeeper client at zetcd and request forwarded to the etcd cluster for best results.

Protocol stuff heavily cribbed from [go-zookeeper](http://github.com/samuel/go-zookeeper/")

## Usage

proxy
```sh
cd proxy
go build
./proxy
```

zkctl
```sh
cd zkctl
go build
./zkctl watch / &
./zkctl put /abc "foo"
```
