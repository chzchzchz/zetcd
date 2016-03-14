package main

import (
	"fmt"
	"net"
	"os"

	"github.com/chzchzchz/zetcd"
	etcd "github.com/coreos/etcd/clientv3"
)

func handle(sp *zetcd.SessionPool, c *etcd.Client, zk net.Conn) {
	s, err := sp.Auth(zk)
	if err != nil {
		fmt.Println(err)
		return
	}
	zke := zetcd.NewZKEtcd(s)
	for err == nil {
		xid, op, operr := zetcd.ReadOp(zk)
		if operr != nil {
			err = operr
			break
		}
		err = zetcd.DispatchZK(zke, xid, op)
	}
	s.Close()
}

func main() {
	fmt.Println("hello word")

	// talk to the etcd3 server
	cfg := etcd.Config{Endpoints: []string{"localhost:2378"}}
	c, err := etcd.New(cfg)

	// listen on zookeeper server port
	ln, err := net.Listen("tcp", ":2181")
	if err != nil {
		os.Exit(-1)
	}
	sp := zetcd.NewSessionPool(c)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("oops %v\n", err)
		}
		go handle(sp, c, conn)
	}
}
