package zetcd

import (
	"encoding/binary"
	"fmt"
	"net"

	etcd "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

type SessionPool struct {
	sessions map[int64]*Session
	c        *etcd.Client
}

type Session struct {
	id        int64
	zkc       net.Conn
	outc      chan []byte
	c         *etcd.Client
	leaseZXid ZXid
	w         *watches

	// stopc is closed to shutdown session
	stopc chan struct{}
	// donec is closed to signal session is torn down
	donec chan struct{}
}

func NewSession(c *etcd.Client, zk net.Conn, id int64) (*Session, error) {
	s := &Session{
		id:    id,
		zkc:   zk,
		outc:  make(chan []byte, 16),
		c:     c,
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}
	s.w = newWatches(s)

	ctx, cancel := context.WithCancel(c.Ctx())
	kach, kaerr := c.KeepAlive(ctx, etcd.LeaseID(id))
	if kaerr != nil {
		return nil, kaerr
	}
	go func() {
		defer func() {
			cancel()
			close(s.outc)
		}()
		for {
			select {
			case ka, ok := <-kach:
				if !ok {
					return
				}
				if ka.Header == nil {
					continue
				}
				s.leaseZXid = ZXid(ka.Header.Revision)
			case <-s.stopc:
				return
			}
		}
	}()

	go func() {
		defer close(s.donec)
		for msg := range s.outc {
			if _, err := s.zkc.Write(msg); err != nil {
				return
			}
		}
	}()

	return s, nil
}

func (s *Session) Send(xid Xid, zxid ZXid, resp interface{}) error {
	buf := make([]byte, 2*1024*1024)
	hdr := &responseHeader{Xid: xid, Zxid: zxid, Err: errOk}

	_, isEv := resp.(*WatcherEvent)
	if isEv {
		hdr.Xid = -1
	}

	ec, hasErr := resp.(*ErrCode)
	if hasErr {
		hdr.Err = *ec
	}
	n1, err1 := encodePacket(buf[4:], hdr)
	if err1 != nil {
		return err1
	}
	pktlen := n1
	if !hasErr {
		n2, err2 := encodePacket(buf[4+n1:], resp)
		if err2 != nil {
			return err2
		}
		pktlen += n2
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(pktlen))
	select {
	case s.outc <- buf[:4+pktlen]:
	case <-s.c.Ctx().Done():
		return s.c.Ctx().Err()
	}
	return nil
}

func (s *Session) Close() {
	close(s.stopc)
	s.w.close()
	<-s.donec
}

func NewSessionPool(client *etcd.Client) *SessionPool {
	return &SessionPool{
		sessions: make(map[int64]*Session),
		c:        client}
}

func (sp *SessionPool) Auth(zk net.Conn) (*Session, error) {
	req := ConnectRequest{}
	ReadPacket(zk, req)
	fmt.Printf("%+v\n", req)

	if req.ProtocolVersion != 0 ||
		req.SessionID != 0 {
		panic("unhandled req stuff!")
	}

	lcr, err := sp.c.Create(sp.c.Ctx(), int64(req.TimeOut)*1000)
	if err != nil {
		return nil, err
	}
	lid := etcd.LeaseID(lcr.ID)

	key := fmt.Sprintf("/zk/ses/%x", lcr.ID)
	_, err = sp.c.Put(sp.c.Ctx(), key, string(req.Passwd), etcd.WithLease(lid))
	if err != nil {
		return nil, err
	}

	// TODO: the session id should be a lease id
	resp := &ConnectResponse{
		ProtocolVersion: 0,
		TimeOut:         int32(lcr.TTL * 1000),
		SessionID:       Sid(lid),
		Passwd:          []byte{}}
	err = WritePacket(zk, resp)

	s, serr := NewSession(sp.c, zk, lcr.ID)
	if serr != nil {
		return nil, serr
	}
	sp.sessions[s.id] = s
	return s, nil
}
