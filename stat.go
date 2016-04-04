package zetcd

import (
	etcd "github.com/coreos/etcd/clientv3"
)

func statGets(p string) []etcd.Op {
	return []etcd.Op{
		etcd.OpGet("/zk/ctime/" + p),
		etcd.OpGet("/zk/mtime/"+p, etcd.WithSort(etcd.SortByModRevision, etcd.SortDescend)),
		etcd.OpGet("/zk/key/" + p),
		etcd.OpGet("/zk/ver/" + p),
		etcd.OpGet("/zk/cver/" + p),
		etcd.OpGet("/zk/aver/" + p),
		etcd.OpGet(getListPfx(p), etcd.WithPrefix()), // to compute num children
	}
}

func statTxn(txnresp *etcd.TxnResponse) (s Stat) {
	ctime := txnresp.Responses[0].GetResponseRange()
	mtime := txnresp.Responses[1].GetResponseRange()
	node := txnresp.Responses[2].GetResponseRange()
	ver := txnresp.Responses[3].GetResponseRange()
	cver := txnresp.Responses[4].GetResponseRange()
	aver := txnresp.Responses[5].GetResponseRange()
	children := txnresp.Responses[6].GetResponseRange()

	// XXX hack: need to format zk / node instead of this garbage
	if len(ctime.Kvs) != 0 {
		s.Ctime = decodeInt64(ctime.Kvs[0].Value)
		s.Czxid = ZXid(ctime.Kvs[0].ModRevision)
	}
	if len(mtime.Kvs) != 0 {
		s.Mzxid = ZXid(mtime.Kvs[0].ModRevision)
		s.Mtime = decodeInt64(mtime.Kvs[0].Value)
	}
	if len(ver.Kvs) != 0 {
		s.Version = Ver(decodeInt64(ver.Kvs[0].Value))
	}
	if len(cver.Kvs) != 0 {
		s.Cversion = Ver(decodeInt64(cver.Kvs[0].Value))
	}
	if len(aver.Kvs) != 0 {
		s.Aversion = Ver(decodeInt64(aver.Kvs[0].Value))
	}
	if len(node.Kvs) != 0 {
		s.EphemeralOwner = Sid(node.Kvs[0].Lease)
		s.DataLength = int32(len(node.Kvs[0].Value))
	}
	s.NumChildren = int32(len(children.Kvs))
	if s.NumChildren > 0 {
		s.Pzxid = ZXid(children.Kvs[0].ModRevision)
	}
	return s
}
