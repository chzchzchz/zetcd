package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/chzchzchz/zetcd"
	etcd "github.com/coreos/etcd/clientv3"
)

func main() {
	etcdaddr := flag.String("endpoint", "localhost:2379", "etcd3 client address")
	zkaddr := flag.String("zkaddr", ":2181", "address for serving zookeeper clients")
	flag.Parse()
	fmt.Println("Running zetcd proxy")

	// talk to the etcd3 server
	cfg := etcd.Config{Endpoints: []string{*etcdaddr}}
	c, err := etcd.New(cfg)

	// listen on zookeeper server port
	ln, err := net.Listen("tcp", *zkaddr)
	if err != nil {
		os.Exit(-1)
	}
	zetcd.Serve(c, ln)
}
