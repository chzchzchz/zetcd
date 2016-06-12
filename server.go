package zetcd

import (
	"net"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

func handle(conn net.Conn, auth AuthFunc) {
	defer conn.Close()
	zke, err := auth(conn)
	for err == nil {
		xid, op, operr := ReadOp(conn)
		if operr != nil {
			err = operr
			break
		}
		err = DispatchZK(zke, xid, op)
	}
	zke.CloseZK()
}

func Serve(ctx context.Context, ln net.Listener, auth AuthFunc) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			glog.V(5).Infof("Accept()=%v", err)
		} else {
			go handle(conn, auth)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
