package zetcd

import (
	"sync"

	etcd "github.com/coreos/etcd/clientv3"
	storagepb "github.com/coreos/etcd/storage/storagepb"
	"golang.org/x/net/context"
)

type watches struct {
	mu sync.Mutex
	s  *Session

	xid2watch map[Xid]*watch

	ctx    context.Context
	cancel context.CancelFunc
}

type watch struct {
	xid    Xid
	evtype EventType
	path   string

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

func (ws *watches) watch(rev ZXid, xid Xid, path string, evtype EventType, cb func(ZXid)) {
	ctx, cancel := context.WithCancel(ws.ctx)
	var wch etcd.WatchChan
	switch evtype {
	case EventNodeDataChanged:
		fallthrough
	case EventNodeCreated:
		fallthrough
	case EventNodeDeleted:
		wch = ws.s.c.Watch(ctx, "/zk/key/"+path, etcd.WithRev(int64(rev)))
	case EventNodeChildrenChanged:
		wch = ws.s.c.Watch(
			ctx,
			getListPfx(path),
			etcd.WithPrefix(),
			etcd.WithRev(int64(rev)))
	default:
		// getchildren case is a little trickier...
		panic("unsupported watch op")
	}

	w := &watch{xid, evtype, path, wch, ctx, cancel, rev, make(chan struct{})}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.xid2watch[xid] = w

	go ws.runWatch(w, cb)
}

func (ws *watches) runWatch(w *watch, cb func(ZXid)) {
	defer func() {
		close(w.donec)
		<-w.wch
		ws.mu.Lock()
		if ws.xid2watch != nil {
			delete(ws.xid2watch, w.xid)
		}
		ws.mu.Unlock()
	}()
	for {
		select {
		case resp, ok := <-w.wch:
			if !ok {
				return
			}
			if w.evtype == EventNodeDeleted {
				isDel := false
				for _, ev := range resp.Events {
					if ev.Type == storagepb.DELETE {
						isDel = true
						break
					}
				}
				if !isDel {
					break
				}
			} else if w.evtype == EventNodeChildrenChanged {
				isChange := false
				for _, ev := range resp.Events {
					if ev.Kv.CreateRevision == 0 ||
						ev.Kv.CreateRevision == ev.Kv.ModRevision {
						isChange = true
						break
					}
				}
				if !isChange {
					break
				}
			}
			cb(ZXid(resp.Header.Revision))
			w.cancel()
		case <-w.ctx.Done():
		}
	}
}

func (ws *watches) close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.cancel()
	for _, w := range ws.xid2watch {
		for range w.wch {
		}
	}
	ws.xid2watch = nil
}

// wait until watcher depending on this completes
// note: path is internal zkpath representation
// TODO: watch waiting may need to be proxy-wide to be correct
// TODO: better algorithm
func (ws *watches) wait(rev ZXid, path string, evtype EventType) {
	ch := []<-chan struct{}{}
	ws.mu.Lock()
	for _, w := range ws.xid2watch {
		if w.path != path {
			continue
		}
		if w.startRev <= rev && w.evtype == evtype {
			ch = append(ch, w.donec)
		}
	}
	ws.mu.Unlock()
	for _, c := range ch {
		<-c
	}
}
