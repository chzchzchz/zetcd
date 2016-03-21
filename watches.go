package zetcd

import (
	"sync"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	etcd "github.com/coreos/etcd/clientv3"
)

type watches struct {
	mu sync.Mutex
	s  *Session

	xid2watch map[Xid]*watch

	ctx    context.Context
	cancel context.CancelFunc
}

type watch struct {
	xid  Xid
	op   Op
	path string

	wch    etcd.WatchChan
	ctx    context.Context
	cancel context.CancelFunc

	// startRev is the etcd store revision when this watch began
	startRev ZXid
	donec    chan struct{}
}

func newWatches(s *Session) *watches {
	ctx, cancel := context.WithCancel(context.TODO())
	return &watches{
		s:         s,
		xid2watch: make(map[Xid]*watch),
		ctx:       ctx,
		cancel:    cancel}
}

func (ws *watches) watch(rev ZXid, xid Xid, path string, op Op, cb func(ZXid)) {
	switch op {
	case opGetData:
	case opExists:
	default:
		// getchildren case is a little trickier...
		panic("unsupported watch op")
	}

	ctx, cancel := context.WithCancel(ws.ctx)
	wch := ws.s.c.Watch(ctx, "/zk/key/"+path, etcd.WithRev(int64(rev+1)))
	w := &watch{xid, op, path, wch, ctx, cancel, rev, make(chan struct{})}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.xid2watch[xid] = w

	go func() {
		defer func() {
			close(w.donec)
			<-w.wch
			ws.mu.Lock()
			delete(ws.xid2watch, xid)
			ws.mu.Unlock()
		}()
		select {
		case resp, ok := <-w.wch:
			if !ok {
				return
			}
			cb(ZXid(resp.Header.Revision))
			w.cancel()
		case <-w.ctx.Done():
		}
	}()
}

func (ws *watches) close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.cancel()
	for _, w := range ws.xid2watch {
		for range w.wch {
		}
		close(w.donec)
	}
	ws.xid2watch = nil
}

// wait until watcher depending on this completes
// TODO: watch waiting may need to be proxy-wide to be correct
// TODO: better algorithm
func (ws *watches) wait(rev ZXid, path string, op Op) {
	ch := []<-chan struct{}{}
	ws.mu.Lock()
	for _, w := range ws.xid2watch {
		if w.path != path {
			continue
		}
		if w.startRev <= rev {
			ch = append(ch, w.donec)
		}
	}
	ws.mu.Unlock()
	for _, c := range ch {
		<-c
	}
}
