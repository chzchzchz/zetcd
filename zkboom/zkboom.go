package main

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	flags = int32(0)
	acl   = zk.WorldACL(zk.PermAll)
)

func main() {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}

	//	benchMem(conn)
	benchPut(conn)
}

func benchMem(conn *zk.Conn) {
	val := make([]byte, 128)

	for i := 0; i < 1000000; i++ {
		_, err := conn.Create("/foo"+fmt.Sprint(i), val, flags, acl)
		if err != nil {
			panic(err)
		}
		if i%1000 == 0 {
			fmt.Println(i)
		}
	}
}

func benchPut(conn *zk.Conn) {
	_, err := conn.Create("/foo", []byte("bar"), flags, acl)
	if err != nil {
		fmt.Println(err)
	}

	donec := make(chan struct{})
	start := time.Now()
	for n := 0; n < 100; n++ {
		go func() {
			for i := 0; i < 1000; i++ {
				_, err = conn.Set("/foo", []byte("bar"), -1)
				if err != nil {
					panic(err)
				}
			}
			donec <- struct{}{}
		}()
	}

	for n := 0; n < 100; n++ {
		<-donec
	}
	fmt.Println(time.Since(start))
}
