package zetcd

import (
	"net"

	etcd "github.com/coreos/etcd/clientv3"
)

type AuthFunc func(net.Conn) (Session, error)
type ZKFunc func(Session) (ZK, error)

func NewAuth(c *etcd.Client) AuthFunc {
	sp := NewSessionPool(c)
	return func(zk net.Conn) (Session, error) {
		s, err := sp.Auth(zk)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
}

func NewZK(c *etcd.Client) ZKFunc {
	return func(s Session) (ZK, error) {
		return NewZKEtcd(c, s), nil
	}
}
