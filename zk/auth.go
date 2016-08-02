package zk

import (
	"github.com/chzchzchz/zetcd"
)

func NewAuth(addrs []string) zetcd.AuthFunc {
	return func(zka zetcd.AuthConn) (zetcd.Session, error) {
		return newSession(addrs, zka)
	}
}

func NewZK() zetcd.ZKFunc {
	return func(s zetcd.Session) (zetcd.ZK, error) {
		zk, err := newZK(s)
		return zetcd.NewZKLog(zk), err
	}
}
