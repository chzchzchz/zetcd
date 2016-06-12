package zetcd

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type SessionPool struct {
	sessions map[etcd.LeaseID]*Session
	c        *etcd.Client
	mu       sync.RWMutex
}

type Session struct {
	id    etcd.LeaseID
	zkc   net.Conn
	outc  chan []byte
	muOut sync.RWMutex
	c     *etcd.Client
	w     *watches

	leaseZXid ZXid
	mu        sync.RWMutex

	// stopc is closed to shutdown session
	stopc chan struct{}
	// donec is closed to signal session is torn down
	donec chan struct{}
}

func NewSession(c *etcd.Client, zk net.Conn, id etcd.LeaseID) (*Session, error) {
	outc := make(chan []byte, 16)
	s := &Session{
		id:    id,
		zkc:   zk,
		outc:  outc,
		c:     c,
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}
	s.w = newWatches(s)

	ctx, cancel := context.WithCancel(c.Ctx())
	kach, kaerr := c.KeepAlive(ctx, id)
	if kaerr != nil {
		return nil, kaerr
	}
	go func() {
		defer func() {
			cancel()
			s.muOut.Lock()
			close(s.outc)
			s.outc = nil
			s.muOut.Unlock()
		}()
		for {
			select {
			case ka, ok := <-kach:
				if !ok {
					return
				}
				if ka.ResponseHeader == nil {
					continue
				}
				s.mu.Lock()
				s.leaseZXid = ZXid(ka.ResponseHeader.Revision)
				s.mu.Unlock()
			case <-s.stopc:
				return
			}
		}
	}()

	go func() {
		defer close(s.donec)
		for msg := range outc {
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
	s.muOut.RLock()
	defer s.muOut.RUnlock()
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

// ZXid gets the lease ZXid
func (s *Session) ZXid() ZXid {
	s.mu.RLock()
	zxid := s.leaseZXid
	s.mu.RUnlock()
	return zxid
}

func NewSessionPool(client *etcd.Client) *SessionPool {
	return &SessionPool{
		sessions: make(map[etcd.LeaseID]*Session),
		c:        client}
}

func (sp *SessionPool) Auth(zk net.Conn) (*Session, error) {
	req := ConnectRequest{}
	ReadPacket(zk, req)
	glog.V(6).Infof("auth(%+v)", req)

	if req.ProtocolVersion != 0 ||
		req.SessionID != 0 {
		panic("unhandled req stuff!")
	}

	lcr, err := sp.c.Grant(sp.c.Ctx(), int64(req.TimeOut)*1000)
	if err != nil {
		return nil, err
	}
	lid := etcd.LeaseID(lcr.ID)

	key := fmt.Sprintf("/zk/ses/%x", lcr.ID)
	_, err = sp.c.Put(sp.c.Ctx(), key, string(req.Passwd), etcd.WithLease(lid))
	if err != nil {
		return nil, err
	}

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
	sp.mu.Lock()
	sp.sessions[s.id] = s
	sp.mu.Unlock()
	return s, nil
}
