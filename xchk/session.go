package xchk

import (
	"fmt"

	"github.com/chzchzchz/zetcd"
)

// session intercepts Sends so responses can be xchked
type session struct {
	zetcd.Conn
	oracle    zetcd.Session
	candidate zetcd.Session
	req       zetcd.ConnectRequest
}

func Auth(zka zetcd.AuthConn, cAuth, oAuth zetcd.AuthFunc) (zetcd.Session, error) {
	xzka := newAuthConn(zka)
	defer xzka.Close()

	var oSession, cSession zetcd.Session
	ozka, czka := xzka.worker(), xzka.worker()
	oerrc, cerrc := make(chan error, 1), make(chan error, 1)

	go func() {
		s, oerr := oAuth(ozka)
		oSession = s
		oerrc <- oerr
	}()
	go func() {
		s, cerr := cAuth(czka)
		cSession = s
		cerrc <- cerr
	}()

	ar, arerr := xzka.Read()
	if arerr != nil {
		return nil, arerr
	}

	xzkc, xerr := xzka.Write(zetcd.AuthResponse{})
	oerr, cerr := <-oerrc, <-cerrc
	if xerr != nil || cerr != nil || oerr != nil {
		return nil, fmt.Errorf("err: xchk: %v. oracle: %v. candidate: %v", oerr, cerr)
	}

	return &session{
		Conn:      xzkc,
		oracle:    oSession,
		candidate: cSession,
		req:       *ar.Req,
	}, nil
}

func (s *session) ConnReq() zetcd.ConnectRequest { return s.req }

func (s *session) Backing() interface{} { return s }

func (s *session) Sid() zetcd.Sid { panic("...") }

func (s *session) Wait(rev zetcd.ZXid, path string, evtype zetcd.EventType) { panic("stub") }

func (s *session) Watch(rev zetcd.ZXid, xid zetcd.Xid, path string, evtype zetcd.EventType, cb func(zetcd.ZXid)) {
	panic("stuB")
}

func (s *session) ZXid() zetcd.ZXid { panic("uh") }
