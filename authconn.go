package zetcd

import (
	"net"

	"github.com/golang/glog"
)

// AuthConn transfers zookeeper handshaking for establishing a session
type AuthConn interface {
	Read() (*AuthRequest, error)
	Write(AuthResponse) (Conn, error)
	Close()
}

type AuthResponse struct {
	Resp *ConnectResponse
	// TODO: add four letter response
}

type AuthRequest struct {
	Req *ConnectRequest
	// TODO: add four letter commands
}

type authConn struct {
	c net.Conn
}

func NewAuthConn(c net.Conn) AuthConn { return &authConn{c} }

func (ac *authConn) Read() (*AuthRequest, error) {
	req := &ConnectRequest{}
	if err := ReadPacket(ac.c, req); err != nil {
		glog.V(6).Infof("error reading connection request (%v)", err)
		return nil, err
	}
	glog.V(6).Infof("auth(%+v)", req)
	return &AuthRequest{req}, nil
}

func (ac *authConn) Write(ar AuthResponse) (Conn, error) {
	if err := WritePacket(ac.c, ar.Resp); err != nil {
		return nil, err
	}
	zkc := NewConn(ac.c)
	ac.c = nil
	return zkc, nil
}

func (ac *authConn) Close() {
	if ac.c != nil {
		ac.c.Close()
	}
}
