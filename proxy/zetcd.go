package main

import (
	"fmt"
	"net"
	"os"

	"github.com/chzchzchz/zetcd"
	etcd "github.com/coreos/etcd/clientv3"
)

func main() {
	fmt.Println("hello word")

	// talk to the etcd3 server
	cfg := etcd.Config{Endpoints: []string{"localhost:2378"}}
	c, err := etcd.New(cfg)

	// listen on zookeeper server port
	ln, err := net.Listen("tcp", ":2181")
	if err != nil {
		os.Exit(-1)
	}
	zetcd.Serve(c, ln)
}
