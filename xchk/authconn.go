package xchk

import (
	"fmt"
	"time"

	"github.com/chzchzchz/zetcd"
)

// authConn implements an AuthConn that can fork off xchked AuthConns
type authConn struct {
	zka     zetcd.AuthConn
	workers []*authConnWorker
}

type reqPkt struct {
	v   *zetcd.AuthRequest
	err error
}

type respPkt struct {
	v   *zetcd.AuthResponse
	err error
}

func newAuthConn(zka zetcd.AuthConn) *authConn {
	return &authConn{zka: zka}
}

func (ac *authConn) Read() (*zetcd.AuthRequest, error) {
	var req reqPkt
	req.v, req.err = ac.zka.Read()
	for _, w := range ac.workers {
		w.reqc <- req
	}
	return req.v, req.err
}

// Write waits for the worker writes and returns a new conn if matching.
func (ac *authConn) Write(ar zetcd.AuthResponse) (zetcd.Conn, error) {
	respPkts := make([]respPkt, len(ac.workers))
	for i, w := range ac.workers {
		select {
		case resp := <-w.respc:
			respPkts[i] = resp
		case <-time.After(time.Second):
			return nil, fmt.Errorf("timed out on worker write")
		}
	}

	var errs []error
	for i := 1; i < len(respPkts); i++ {
		vNil := respPkts[0].v == nil && respPkts[i].v == nil
		vVal := respPkts[0].v != nil && respPkts[i].v != nil
		if !vNil && !vVal {
			err := fmt.Errorf("mismatch [0]=%+v vs [%d]=%+v", respPkts[0].v, i, respPkts[i].v)
			errs = append(errs, err)
		}
		vNoErr := respPkts[0].err == nil && respPkts[1].err == nil
		vErr := respPkts[0].err != nil && respPkts[1].err != nil
		if !vNoErr && !vErr {
			err := fmt.Errorf("mismatch [0]=%+v vs [%d]=%+v", respPkts[0].err, i, respPkts[i].err)
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return nil, fmt.Errorf("%+v", errs)
	}

	zkc, cerr := ac.zka.Write(*respPkts[0].v)
	if cerr != nil {
		return nil, cerr
	}

	conn, workers := newConn(zkc, len(ac.workers))
	for i, w := range ac.workers {
		w.connc <- workers[i]
	}
	return conn, nil
}

func (ac *authConn) Close() {
	ac.zka.Close()
	for _, w := range ac.workers {
		w.Close()
	}
}

// authConnWorker implements an AuthConn that is xchked by an authConn
type authConnWorker struct {
	reqc  chan reqPkt
	respc chan respPkt
	connc chan zetcd.Conn
}

// worker creates a clone of the auth conn
func (ac *authConn) worker() *authConnWorker {
	acw := &authConnWorker{
		reqc:  make(chan reqPkt, 1),
		respc: make(chan respPkt, 1),
		connc: make(chan zetcd.Conn),
	}
	ac.workers = append(ac.workers, acw)
	return acw
}

func (acw *authConnWorker) Read() (*zetcd.AuthRequest, error) {
	pkt := <-acw.reqc
	return pkt.v, pkt.err
}

func (acw *authConnWorker) Write(ar zetcd.AuthResponse) (zetcd.Conn, error) {
	acw.respc <- respPkt{&ar, nil}
	c := <-acw.connc
	if c == nil {
		return nil, fmt.Errorf("xchk error")
	}
	return c, nil
}

func (acw *authConnWorker) Close() {
	if acw.respc != nil {
		close(acw.respc)
		acw.respc = nil
	}
}
