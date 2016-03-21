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

func TestCreateGet(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, _, err := c.Get("/abc"); err == nil {
			t.Fatalf("expected error on getting absent /abc")
		}
		if _, err := c.Create("/foo/bar", []byte("x"), 0, nil); err == nil {
			t.Fatalf("expected error on creating /foo/bar without /foo")
		}
		if _, err := c.Create("/abc", []byte("data1"), 0, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc", []byte("data1"), 0, nil); err == nil {
			t.Fatalf("don't allow double create")
		}
		if _, _, err := c.Get("/abc"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Get("/abc/def"); err == nil {
			t.Fatalf("expected error on getting /abc/def")
		}
		if _, err := c.Create("/abc/def", []byte("data2"), 0, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Get("/abc/def"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSync(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Sync("/abc"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestExists(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, nil); err != nil {
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

func TestChildren(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/def", []byte(""), 0, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/123", []byte(""), 0, nil); err != nil {
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
		Serve(c, ln)
		close(ch)
	}()
	return ch, func() { ln.Close() }
}
