package zetcd

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"

	etcd "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

type SessionPool struct {
	sessions map[etcd.LeaseID]Session
	c        *etcd.Client
	mu       sync.RWMutex
}

type Session interface {
	Conn
	Watches
	Sid() Sid
	ZXid() ZXid
	ConnReq() ConnectRequest
	Backing() interface{}
}

type session struct {
	Conn
	*watches
	id  etcd.LeaseID
	c   *etcd.Client
	req ConnectRequest

	leaseZXid ZXid
	mu        sync.RWMutex
}

func (s *session) ConnReq() ConnectRequest { return s.req }
func (s *session) Backing() interface{}    { return s }

func newSession(c *etcd.Client, zkc Conn, id etcd.LeaseID) (*session, error) {
	ctx, cancel := context.WithCancel(c.Ctx())
	s := &session{Conn: zkc, id: id, c: c, watches: newWatches(c)}

	kach, kaerr := c.KeepAlive(ctx, id)
	if kaerr != nil {
		cancel()
		return nil, kaerr
	}

	go func() {
		defer func() {
			cancel()
			s.Close()
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
			case <-s.StopNotify():
				return
			}
		}
	}()

	return s, nil
}

func (s *session) Sid() Sid { return Sid(s.id) }

// ZXid gets the lease ZXid
func (s *session) ZXid() ZXid {
	s.mu.RLock()
	zxid := s.leaseZXid
	s.mu.RUnlock()
	return zxid
}

func NewSessionPool(client *etcd.Client) *SessionPool {
	return &SessionPool{
		sessions: make(map[etcd.LeaseID]Session),
		c:        client}
}

func (sp *SessionPool) newSessionLease(req *ConnectRequest) (etcd.LeaseID, error) {
	var pwd []byte
	if len(req.Passwd) == 0 {
		pwd = make([]byte, 16)
		if _, err := rand.Read(pwd); err != nil {
			return 0, err
		}
	}
	lcr, err := sp.c.Grant(sp.c.Ctx(), int64(req.TimeOut)*1000)
	if err != nil {
		return 0, err
	}
	_, err = sp.c.Put(sp.c.Ctx(), lid2key(lcr.ID), string(pwd), etcd.WithLease(lcr.ID))
	if err != nil {
		return 0, err
	}
	return lcr.ID, nil
}

func lid2key(lid etcd.LeaseID) string { return fmt.Sprintf("/zk/ses/%x", lid) }

func (sp *SessionPool) Auth(zka AuthConn) (Session, error) {
	defer zka.Close()
	areq, err := zka.Read()
	if err != nil {
		return nil, err
	}
	req := areq.Req

	if req.ProtocolVersion != 0 {
		panic(fmt.Sprintf("unhandled req stuff! %+v", req))
	}

	lid := etcd.LeaseID(req.SessionID)
	// TODO use ttl from lease
	ttl := req.TimeOut
	if lid == 0 {
		if lid, err = sp.newSessionLease(req); err != nil {
			return nil, err
		}
	} else {
		gresp, gerr := sp.c.Get(sp.c.Ctx(), lid2key(lid))
		if gerr != nil {
			return nil, gerr
		}
		if len(gresp.Kvs) == 0 {
			return nil, fmt.Errorf("bad lease")
		}
		if bytes.Compare(gresp.Kvs[0].Value, req.Passwd) != 0 {
			return nil, fmt.Errorf("bad passwd")
		}
	}

	resp := &ConnectResponse{
		ProtocolVersion: 0,
		TimeOut:         int32(ttl),
		SessionID:       Sid(lid),
		Passwd:          []byte{}}
	zkc, aerr := zka.Write(AuthResponse{Resp: resp})
	if zkc == nil || aerr != nil {
		return nil, aerr
	}

	s, serr := newSession(sp.c, zkc, lid)
	if serr != nil {
		return nil, serr
	}
	s.req = *areq.Req

	sp.mu.Lock()
	sp.sessions[s.id] = s
	sp.mu.Unlock()
	return s, nil
}
