package zk

import (
	"net"

	"github.com/chzchzchz/zetcd"
)

func NewAuth(addrs []string) zetcd.AuthFunc {
	return func(zk net.Conn) (zetcd.Session, error) {
		return newSession(addrs, zk)
	}
}

func NewZK() zetcd.ZKFunc {
	return func(s zetcd.Session) (zetcd.ZK, error) {
		zk, err := newZK(s)
		return zetcd.NewZKLog(zk), err
	}
}
