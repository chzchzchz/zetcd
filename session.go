package zetcd

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type SessionPool struct {
	sessions map[etcd.LeaseID]Session
	c        *etcd.Client
	mu       sync.RWMutex
	be       sessionBackend
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
	be, err := newAesSessionBackend(client)
	if err != nil {
		panic(err)
	}
	return &SessionPool{
		sessions: make(map[etcd.LeaseID]Session),
		c:        client,
		be:       be,
	}
}

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

	// TODO use ttl from lease
	lid := etcd.LeaseID(req.SessionID)
	if lid == 0 {
		lid, req.Passwd, err = sp.be.create(int64(req.TimeOut) / 1000)
	} else {
		lid, err = sp.be.resume(req.SessionID, req.Passwd)
	}

	if err != nil {
		resp := &ConnectResponse{Passwd: make([]byte, 14)}
		zkc, _ := zka.Write(AuthResponse{Resp: resp})
		if zkc != nil {
			zkc.Close()
		}
		return nil, err
	}

	resp := &ConnectResponse{
		ProtocolVersion: 0,
		TimeOut:         req.TimeOut,
		SessionID:       Sid(lid),
		Passwd:          req.Passwd,
	}
	glog.V(7).Infof("authresp=%+v", resp)
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

type sessionBackend interface {
	create(ttl int64) (etcd.LeaseID, []byte, error)
	resume(Sid, []byte) (etcd.LeaseID, error)
}

type etcdSessionBackend struct {
	c *etcd.Client
}

func (sp *etcdSessionBackend) create(ttl int64) (etcd.LeaseID, []byte, error) {
	pwd := make([]byte, 16)
	if _, err := rand.Read(pwd); err != nil {
		return 0, nil, err
	}
	if ttl == 0 {
		ttl = 1
	}
	lcr, err := sp.c.Grant(sp.c.Ctx(), ttl)
	if err != nil {
		return 0, nil, err
	}
	_, err = sp.c.Put(sp.c.Ctx(), lid2key(lcr.ID), string(pwd), etcd.WithLease(lcr.ID))
	if err != nil {
		return 0, nil, err
	}
	return lcr.ID, pwd, nil
}

func (sp *etcdSessionBackend) resume(sid Sid, pwd []byte) (etcd.LeaseID, error) {
	gresp, gerr := sp.c.Get(sp.c.Ctx(), lid2key(etcd.LeaseID(sid)))
	switch {
	case gerr != nil:
		return 0, gerr
	case len(gresp.Kvs) == 0:
		return 0, fmt.Errorf("bad lease")
	case bytes.Compare(gresp.Kvs[0].Value, pwd) != 0:
		return 0, fmt.Errorf("bad passwd")
	}
	return etcd.LeaseID(sid), nil
}

func lid2key(lid etcd.LeaseID) string { return fmt.Sprintf("/zk/ses/%x", lid) }

type aesSessionBackend struct {
	c   *etcd.Client
	key []byte
	b   cipher.Block
}

func newAesSessionBackend(c *etcd.Client) (sb *aesSessionBackend, err error) {
	sb = &aesSessionBackend{c: c, key: make([]byte, 16)}
	if _, err = rand.Read(sb.key); err != nil {
		return nil, err
	}
	if sb.b, err = aes.NewCipher(sb.key); err != nil {
		return nil, err
	}
	return sb, nil
}

func (sb *aesSessionBackend) create(ttl int64) (etcd.LeaseID, []byte, error) {
	if ttl == 0 {
		ttl = 1
	}
	lcr, err := sb.c.Grant(sb.c.Ctx(), ttl)
	if err != nil {
		return 0, nil, err
	}
	return lcr.ID, sb.sid2pwd(Sid(lcr.ID)), nil
}

func (sb *aesSessionBackend) resume(sid Sid, pwd []byte) (etcd.LeaseID, error) {
	if bytes.Compare(sb.sid2pwd(sid), pwd) != 0 {
		return 0, fmt.Errorf("bad password")
	}
	return etcd.LeaseID(sid), nil
}

func (sb *aesSessionBackend) sid2pwd(sid Sid) []byte {
	dst, src := make([]byte, sb.b.BlockSize()), make([]byte, sb.b.BlockSize())
	binary.BigEndian.PutUint64(src, uint64(sid))
	sb.b.Encrypt(dst, src)
	return dst
}
