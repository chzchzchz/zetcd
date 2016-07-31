package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"golang.org/x/net/context"
	"github.com/chzchzchz/zetcd"
	"github.com/chzchzchz/zetcd/zk"
	etcd "github.com/coreos/etcd/clientv3"
)

func main() {
	etcdaddr := flag.String("endpoint", "localhost:2379", "etcd3 client address")
	zkaddr := flag.String("zkaddr", ":2181", "address for serving zookeeper clients")
	oracleAddr := flag.String("zkoracle", "", "oracle zookeeper server address")
	bridgeAddr := flag.String("zkbridge", "", "bridge zookeeper server address (for debugging)")

	flag.Parse()
	fmt.Println("Running zetcd proxy")

	// listen on zookeeper server port
	ln, err := net.Listen("tcp", *zkaddr)
	if err != nil {
		os.Exit(-1)
	}

	var authf zetcd.AuthFunc
	var zkf zetcd.ZKFunc
	var ctx context.Context

	if bridgeAddr == nil {
		// talk to the etcd3 server
		cfg := etcd.Config{Endpoints: []string{*etcdaddr}}
		c, err := etcd.New(cfg)
		if err != nil {
			panic(err)
		}
		authf = zetcd.NewAuth(c)
		zkf = zetcd.NewZK(c)
		if oracleAddr != nil {
			panic("oops")
			// auth = xchk.NewAuth(auth)
			// zkf = xchk.NewZK(*oracleaddr, zkf)
		}
		ctx = c.Ctx()
	} else {
		// boring bridge for testing
		authf = zk.NewAuth([]string{*bridgeAddr})
		zkf = zk.NewZK()
		ctx = context.Background()
	}
	zetcd.Serve(ctx, ln, authf, zkf)
}
