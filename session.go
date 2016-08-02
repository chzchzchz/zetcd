package zetcd

import (
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

func (sp *SessionPool) Auth(zka AuthConn) (Session, error) {
	defer zka.Close()
	areq, err := zka.Read()
	if err != nil {
		return nil, err
	}
	req := areq.Req

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
	zkc, aerr := zka.Write(AuthResponse{Resp: resp})
	if zkc == nil || aerr != nil {
		return nil, aerr
	}

	s, serr := newSession(sp.c, zkc, lcr.ID)
	if serr != nil {
		return nil, serr
	}
	s.req = *areq.Req

	sp.mu.Lock()
	sp.sessions[s.id] = s
	sp.mu.Unlock()
	return s, nil
}
