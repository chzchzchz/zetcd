package zetcd

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	zkAddr    = "127.0.0.1:2181"
	zetcdAddr = "127.0.0.1:2182"
)

func init() { zk.DefaultLogger = log.New(ioutil.Discard, "", 0) }

func benchGet(b *testing.B, addr string) {
	c, _, err := zk.Connect([]string{addr}, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	c.Create("/abc", []byte("abc"), 0, acl)
	for i := 0; i < b.N; i++ {
		if _, _, gerr := c.Get("/abc"); gerr != nil {
			b.Fatal(err)
		}
	}
}

func benchConnGet(b *testing.B, addr string) {
	for i := 0; i < b.N; i++ {
		c, _, err := zk.Connect([]string{addr}, time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, gerr := c.Get("/abc"); gerr != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func benchCreateSet(b *testing.B, addr string) {
	c, _, err := zk.Connect([]string{addr}, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf("/%d", i)
		v := fmt.Sprintf("%v", time.Now())
		c.Create(s, []byte(v), 0, acl)
		c.Set("/", []byte(v), -1)
	}
}

func BenchmarkZetcdGet(b *testing.B) { benchGet(b, zetcdAddr) }
func BenchmarkZKGet(b *testing.B)    { benchGet(b, zkAddr) }

func BenchmarkZetcdConnGet(b *testing.B) { benchConnGet(b, zetcdAddr) }
func BenchmarkZKConnGet(b *testing.B)    { benchConnGet(b, zkAddr) }

func BenchmarkZetcdCreateSet(b *testing.B) { benchCreateSet(b, zetcdAddr) }
func BenchmarkZKCreateSet(b *testing.B)    { benchCreateSet(b, zkAddr) }
