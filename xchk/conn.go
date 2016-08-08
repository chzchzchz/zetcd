package xchk

import (
	"fmt"

	"github.com/chzchzchz/zetcd"
	"github.com/golang/glog"
)

// conn implements a Conn that xchks several conns
type conn struct {
	zkc   zetcd.Conn
	stopc chan struct{}
	donec chan struct{}

	readc chan zetcd.ZKRequest
	sendc chan sendPkt

	workers []*connWorker
}

func newConn(zkc zetcd.Conn, nworkers int) (*conn, []zetcd.Conn) {
	c := &conn{
		zkc:   zkc,
		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		readc:   make(chan zetcd.ZKRequest, 16),
		sendc:   make(chan sendPkt, 16),
		workers: make([]*connWorker, nworkers),
	}

	workers := make([]zetcd.Conn, len(c.workers))
	for i := range c.workers {
		c.workers[i] = &connWorker{
			readc:  make(chan zetcd.ZKRequest, 16),
			stopc:  make(chan struct{}),
			parent: c,
		}
		workers[i] = c.workers[i]
	}

	// collect sends from workers
	go c.sendLoop()
	return c, workers
}

func (c *conn) sendLoop() {
	// catch OOB messages from servers, propagate back to client on dupe
	for {
		var sp sendPkt
		select {
		case sp = <-c.sendc:
		case <-c.stopc:
			return
		}
		glog.V(7).Infof("XchkSendLoop: %+v", sp)
	}
}

func (c *conn) Send(xid zetcd.Xid, zxid zetcd.ZXid, resp interface{}) error {
	glog.V(6).Infof("sendXchk Xid:%v ZXid:%v Resp:%+v", xid, zxid, resp)
	return c.zkc.Send(xid, zxid, resp)
}

func (c *conn) Read() <-chan zetcd.ZKRequest { return c.zkc.Read() }
func (c *conn) StopNotify() <-chan struct{}  { return c.stopc }

func (c *conn) Close() {
	close(c.stopc)
	<-c.donec
}

type connWorker struct {
	parent *conn
	readc  chan zetcd.ZKRequest

	stopc chan struct{}
}

type sendPkt struct {
	xid  zetcd.Xid
	zxid zetcd.ZXid
	resp interface{}
}

func (c *connWorker) Send(xid zetcd.Xid, zxid zetcd.ZXid, resp interface{}) error {
	glog.V(7).Infof("connWorkerSend(%v,%v,%+v)", xid, zxid, resp)
	select {
	case c.parent.sendc <- sendPkt{xid, zxid, resp}:
	case <-c.stopc:
		return fmt.Errorf("send stopped")
	}
	return nil
}

func (c *connWorker) Read() <-chan zetcd.ZKRequest { return c.readc }
func (c *connWorker) StopNotify() <-chan struct{}  { return c.stopc }
func (c *connWorker) Close()                       { close(c.stopc) }
