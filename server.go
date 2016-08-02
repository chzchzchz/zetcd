package zetcd

import (
	"net"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

func handle(conn net.Conn, auth AuthFunc, zk ZKFunc) {
	s, serr := auth(NewAuthConn(conn))
	if serr != nil {
		return
	}
	zke, zkerr := zk(s)
	if zkerr != nil {
		s.Close()
		return
	}
	defer zke.CloseZK()
	for zkreq := range s.Read() {
		glog.V(9).Infof("zkreq=%+v", zkreq)
		if zkreq.err != nil {
			break
		}
		if derr := DispatchZK(zke, zkreq.xid, zkreq.req); derr != nil {
			break
		}
	}
}

func Serve(ctx context.Context, ln net.Listener, auth AuthFunc, zk ZKFunc) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			glog.V(5).Infof("Accept()=%v", err)
		} else {
			go handle(conn, auth, zk)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
