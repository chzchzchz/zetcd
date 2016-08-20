package zetcd

import (
	"net"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

func handle(conn net.Conn, auth AuthFunc, zk ZKFunc) {
	glog.V(6).Infof("accepted remote connection %q", conn.RemoteAddr())
	s, serr := auth(NewAuthConn(conn))
	if serr != nil {
		return
	}
	zke, zkerr := zk(s)
	if zkerr != nil {
		s.Close()
		return
	}
	defer func() {
		glog.V(6).Infof("closing remote connection %q", conn.RemoteAddr())
		zke.CloseZK()
	}()
	for zkreq := range s.Read() {
		glog.V(9).Infof("zkreq=%v", &zkreq)
		if zkreq.err != nil {
			break
		}
		zkresp := DispatchZK(zke, zkreq.xid, zkreq.req)
		if zkresp.Err != nil {
			glog.V(9).Infof("dispatch error", zkresp.Err)
			break
		}
		if zkresp.Hdr.Err == 0 {
			s.Send(zkresp.Hdr.Xid, zkresp.Hdr.Zxid, zkresp.Resp)
		} else {
			s.Send(zkresp.Hdr.Xid, zkresp.Hdr.Zxid, &zkresp.Hdr.Err)
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
