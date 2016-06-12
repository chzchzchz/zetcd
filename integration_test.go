package zetcd

import (
	"net"
	"os"
	"testing"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	acl = zk.WorldACL(zk.PermAll)
)

func TestCreateGet(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, _, err := c.Get("/abc"); err == nil {
			t.Fatalf("expected error on getting absent /abc")
		}
		if _, err := c.Create("/foo/bar", []byte("x"), 0, acl); err == nil {
			t.Fatalf("expected error on creating /foo/bar without /foo")
		}
		if _, err := c.Create("/abc", []byte("data1"), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc", []byte("data1"), 0, acl); err == nil {
			t.Fatalf("don't allow double create")
		}
		if _, _, err := c.Get("/abc"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Get("/abc/def"); err == nil {
			t.Fatalf("expected error on getting /abc/def")
		}
		if _, err := c.Create("/abc/def", []byte("data2"), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Get("/abc/def"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestGetDataW(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte("data1"), 0, acl); err != nil {
			t.Fatal(err)
		}
		_, _, ch, werr := c.GetW("/abc")
		if werr != nil {
			t.Fatal(werr)
		}
		select {
		case <-ch:
			t.Fatalf("should block on get channel")
		case <-time.After(10 * time.Millisecond):
		}
		if _, err := c.Set("/abc", []byte("a"), -1); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("took too long to get data update")
		}
	})
}

func TestSync(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Sync("/abc"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestExists(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if ok, _, err := c.Exists("/abc"); err != nil || !ok {
			t.Fatalf("expected it to exist %v %v", err, ok)
		}
		if ok, _, err := c.Exists("/ab"); ok {
			t.Fatalf("expected it to not exist %v %v", err, ok)
		}
	})
}

func TestExistsW(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		// test create
		ok, _, ch, err := c.ExistsW("/abc")
		if ok || err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("took too long to get creation exists event")
		}

		// test (multi) set
		for i := 0; i < 2; i++ {
			ok, _, ch, err = c.ExistsW("/abc")
			if !ok || err != nil {
				t.Fatal(err)
			}
			if _, err := c.Set("/abc", []byte("a"), -1); err != nil {
				t.Fatal(err)
			}
			select {
			case <-ch:
				t.Fatalf("set data shouldn't trigger watcher")
			case <-time.After(time.Second):
			}
		}

		// test delete
		ok, _, ch, err = c.ExistsW("/abc")
		if !ok || err != nil {
			t.Fatal(err)
		}
		if err = c.Delete("/abc", -1); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("took too long to get deletion exists event")
		}
	})
}

func TestChildren(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/def", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/123", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		children, _, err := c.Children("/")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 1 {
			t.Fatalf("expected one child, got %v", children)
		}
		children, _, err = c.Children("/abc")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 2 {
			t.Fatalf("expected two children, got %v", children)
		}
	})
}

func TestGetChildrenW(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}

		// watch for /abc/def
		_, _, ch, err := c.ChildrenW("/abc")
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
			t.Fatalf("should block")
		case <-time.After(10 * time.Millisecond):
		}
		if _, err := c.Create("/abc/def", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("waited to long for new child")
		}

		// watch for /abc/123
		_, _, ch, err = c.ChildrenW("/abc")
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
			t.Fatalf("should block")
		case <-time.After(10 * time.Millisecond):
		}
		if _, err := c.Create("/abc/123", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("waited to long for new child")
		}
	})
}

func TestCreateInvalidACL(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		werr := ErrInvalidACL
		resp, err := c.Create("/foo", []byte("x"), 0, nil)
		if err == nil {
			t.Fatalf("created with invalid acl %v, wanted %v", resp, werr)
		}
		if err.Error() != werr.Error() {
			t.Fatalf("got err %v, wanted %v", err, werr)
		}
	})
}

func runTest(t *testing.T, f func(*testing.T, *zk.Conn)) {
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ch, cancel := serve(clus.RandClient())

	c, _, err := zk.Connect([]string{"127.0.0.1:30000"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f(t, c)

	clus.Terminate(t)
	cancel()
	<-ch

}

func serve(c *etcd.Client) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	// TODO use unix socket
	ln, err := net.Listen("tcp", ":30000")
	if err != nil {
		os.Exit(-1)
	}
	go func() {
		Serve(c.Ctx(), ln, NewAuth(c))
		close(ch)
	}()
	return ch, func() { ln.Close() }
}
