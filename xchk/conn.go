package xchk

import (
	"fmt"
	"sync"
	"time"

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

	// mu protects pktmap
	mu sync.Mutex

	// oobRespPath tracks out of band events by path
	oobRespPath map[string]chan sendPkt

	workers []*connWorker
}

func newConn(zkc zetcd.Conn, nworkers int) (*conn, []zetcd.Conn) {
	c := &conn{
		zkc:   zkc,
		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		readc: make(chan zetcd.ZKRequest, 16),
		sendc: make(chan sendPkt, 16),

		oobRespPath: make(map[string]chan sendPkt),

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

func (c *conn) processSendOOB(sp sendPkt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.oobRespPath == nil {
		return
	}

	if sp.xid != -1 {
		panic("expected xid == -1")
	}

	if lastCh := c.oobRespPath[sp.wev.Path]; lastCh != nil {
		lastCh <- sp
		delete(c.oobRespPath, sp.wev.Path)
		return
	}
	ch := make(chan sendPkt, 1)
	c.oobRespPath[sp.wev.Path] = ch

	go func() {
		var newSp sendPkt
		var ok bool
		select {
		case newSp, ok = <-ch:
			if !ok {
				return
			}
		case <-time.After(3 * time.Second):
			glog.Warningf("xchk failed waited too long to match response to %+v", *sp.wev)
			newSp = sp
		}
		c.mu.Lock()
		if c.oobRespPath != nil {
			delete(c.oobRespPath, sp.wev.Path)
		}
		c.mu.Unlock()
		if *newSp.wev != *sp.wev {
			glog.Warningf("xchk failed (path:%q): %+v != %+v", sp.wev.Path, *sp.wev, *newSp.wev)
		}
		glog.V(6).Infof("xchkSendOOB response %+v", *sp.wev)
		c.zkc.Send(sp.xid, sp.zxid, sp.wev)
	}()
}

func (c *conn) sendLoop() {
	for {
		var sp sendPkt
		select {
		case sp = <-c.sendc:
		case <-c.stopc:
			return
		}
		c.processSendOOB(sp)
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
	c.mu.Lock()
	for _, ch := range c.oobRespPath {
		close(ch)
	}
	c.oobRespPath = nil
	c.mu.Unlock()
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
	wev  *zetcd.WatcherEvent
}

func (c *connWorker) Send(xid zetcd.Xid, zxid zetcd.ZXid, resp interface{}) error {
	glog.V(7).Infof("connWorkerSend(%v,%v,%+v)", xid, zxid, resp)

	wev, ok := resp.(*zetcd.WatcherEvent)
	if !ok {
		glog.Fatalf("unexpected send response %+v", resp)
	}

	select {
	case c.parent.sendc <- sendPkt{xid, zxid, wev}:
	case <-c.stopc:
		return fmt.Errorf("send stopped")
	}
	return nil
}

func (c *connWorker) Read() <-chan zetcd.ZKRequest { return c.readc }
func (c *connWorker) StopNotify() <-chan struct{}  { return c.stopc }
func (c *connWorker) Close()                       { close(c.stopc) }
