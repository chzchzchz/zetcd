package zetcd

import (
	etcd "github.com/coreos/etcd/clientv3"
)

func statGets(p string) []etcd.Op {
	return []etcd.Op{
		etcd.OpGet("/zk/ctime/"+p, etcd.WithSerializable()),
		etcd.OpGet("/zk/mtime/"+p, etcd.WithSerializable(),
			etcd.WithSort(etcd.SortByModRevision, etcd.SortDescend)),
		etcd.OpGet("/zk/key/"+p, etcd.WithSerializable()),
		etcd.OpGet("/zk/cver/"+p, etcd.WithSerializable()),
		etcd.OpGet("/zk/aver/"+p, etcd.WithSerializable()),
		// to compute num children
		etcd.OpGet(getListPfx(p), etcd.WithSerializable(), etcd.WithPrefix()),
	}
}

func statTxn(txnresp *etcd.TxnResponse) (s Stat) {
	ctime := txnresp.Responses[0].GetResponseRange()
	mtime := txnresp.Responses[1].GetResponseRange()
	node := txnresp.Responses[2].GetResponseRange()
	cver := txnresp.Responses[3].GetResponseRange()
	aver := txnresp.Responses[4].GetResponseRange()
	children := txnresp.Responses[5].GetResponseRange()

	// XXX hack: need to format zk / node instead of this garbage
	if len(ctime.Kvs) != 0 {
		s.Ctime = decodeInt64(ctime.Kvs[0].Value)
		s.Czxid = ZXid(ctime.Kvs[0].ModRevision)
		s.Pzxid = s.Czxid
	}
	if len(mtime.Kvs) != 0 {
		s.Mzxid = ZXid(mtime.Kvs[0].ModRevision)
		s.Mtime = decodeInt64(mtime.Kvs[0].Value)
		s.Version = Ver(mtime.Kvs[0].Version - 1)
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
