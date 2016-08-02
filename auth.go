package zetcd

import (
	etcd "github.com/coreos/etcd/clientv3"
)

type AuthFunc func(AuthConn) (Session, error)
type ZKFunc func(Session) (ZK, error)

func NewAuth(c *etcd.Client) AuthFunc {
	sp := NewSessionPool(c)
	return func(zka AuthConn) (Session, error) {
		return sp.Auth(zka)
	}
}

func NewZK(c *etcd.Client) ZKFunc {
	return func(s Session) (ZK, error) {
		return NewZKLog(NewZKEtcd(c, s)), nil
	}
}
