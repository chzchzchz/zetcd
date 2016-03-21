package zetcd

import (
	"fmt"
	"net"

	etcd "github.com/coreos/etcd/clientv3"
)

func handle(sp *SessionPool, c *etcd.Client, zk net.Conn) {
	s, err := sp.Auth(zk)
	if err != nil {
		fmt.Println(err)
		return
	}
	zke := NewZKEtcd(s)
	for err == nil {
		xid, op, operr := ReadOp(zk)
		if operr != nil {
			err = operr
			break
		}
		err = DispatchZK(zke, xid, op)
	}
	s.Close()
}

func Serve(c *etcd.Client, ln net.Listener) {
	sp := NewSessionPool(c)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("oops %v\n", err)
		} else {
			go handle(sp, c, conn)
		}
		select {
		case <-c.Ctx().Done():
			return
		default:
		}
	}
}
