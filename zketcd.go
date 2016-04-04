package zetcd

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"path"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	v3sync "github.com/coreos/etcd/clientv3/concurrency"
)

type zkEtcd struct {
	s *Session
}

func NewZKEtcd(s *Session) ZK {
	return &zkEtcd{s}
}

func (z *zkEtcd) Create(xid Xid, op *CreateRequest) error {
	opts := []etcd.OpOption{}
	switch op.Flags {
	case 0:
	case FlagEphemeral:
		opts = append(opts, etcd.WithLease(etcd.LeaseID(z.s.id)))
	default:
		// support seq flag
		panic("unsupported create flags")
	}

	p := mkPath(op.Path)
	pp := mkPath(path.Dir(op.Path))
	key := "/zk/key/" + p
	pkey := "/zk/cver/" + pp

	applyf := func(s v3sync.STM) error {
		if len(op.Acl) == 0 {
			return ErrInvalidACL
		}
		if s.Rev(pkey) == 0 && len(pp) != 2 {
			// no parent
			return ErrNoNode
		}
		if s.Rev("/zk/ver/"+p) != 0 {
			return ErrNodeExists
		}

		t := encodeTime()

		nextCVer := encodeInt64(decodeInt64([]byte(s.Get(pkey))) + 1)
		s.Put("/zk/cver/"+pp, nextCVer)
		s.Put("/zk/mtime/"+pp, t)

		s.Put(key, string(op.Data), opts...)
		s.Put("/zk/ctime/"+p, t, opts...)
		s.Put("/zk/mtime/"+p, t, opts...)
		s.Put("/zk/ver/"+p, encodeInt64(0), opts...)
		s.Put("/zk/cver/"+p, encodeInt64(0), opts...)
		s.Put("/zk/aver/"+p, encodeInt64(0), opts...)
		s.Put("/zk/acl/"+p, encodeACLs(op.Acl), opts...)

		return nil
	}

	resp, err := v3sync.NewSTMSerializable(z.s.c.Ctx(), z.s.c, applyf)
	errResp := errOk
	// XXX do I need valid zxid on errors?
	switch err {
	case ErrNoNode:
		// parent missing
		errResp = errNoNode
	case ErrNodeExists:
		// this key already exists
		errResp = errNodeExists
	case ErrInvalidACL:
		errResp = errInvalidAcl
	case nil:
	default:
		return err
	}

	if errResp != errOk {
		return z.s.Send(xid, 0, &errResp)
	}

	xzid := ZXid(resp.Header.Revision)
	z.s.w.wait(xzid, p, EventNodeCreated)
	return z.s.Send(xid, xzid, &CreateResponse{op.Path})
}

func (z *zkEtcd) GetChildren2(xid Xid, op *GetChildren2Request) error {
	resp := &GetChildren2Response{}
	p := mkPath(op.Path)

	txnresp, err := z.s.c.Txn(z.s.c.Ctx()).Then(statGets(p)...).Commit()
	if err != nil {
		return err
	}

	resp.Stat = statTxn(txnresp)
	if len(p) != 2 && resp.Stat.Ctime == 0 {
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, ZXid(txnresp.Header.Revision), &errResp)
	}

	children := txnresp.Responses[6].GetResponseRange()
	for _, kv := range children.Kvs {
		zkkey := strings.Replace(string(kv.Key), getListPfx(p), "", 1)
		resp.Children = append(resp.Children, zkkey)
	}

	zxid := ZXid(children.Header.Revision)
	z.s.w.wait(zxid, p, EventNodeChildrenChanged)

	if op.Watch {
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  EventNodeChildrenChanged,
				State: StateConnected,
				Path:  op.Path,
			}
			z.s.Send(xid, newzxid, wresp)
		}
		z.s.w.watch(zxid, xid, p, EventNodeChildrenChanged, f)
	}

	return z.s.Send(xid, zxid, resp)
}

func (z *zkEtcd) Ping(xid Xid, op *PingRequest) error {
	return z.s.Send(xid, z.s.leaseZXid, &PingResponse{})
}

func (z *zkEtcd) Delete(xid Xid, op *DeleteRequest) error {
	p := mkPath(op.Path)
	pp := mkPath(path.Dir(op.Path))
	key := "/zk/key/" + p
	pkey := "/zk/cver/" + pp

	applyf := func(s v3sync.STM) error {
		if s.Rev(pkey) == 0 && len(pp) != 2 {
			// no parent
			return ErrNoNode
		}
		if s.Rev("/zk/ver/"+p) == 0 {
			return ErrNoNode
		}
		ver := Ver(decodeInt64([]byte(s.Get("/zk/ver/" + p))))
		if op.Version != Ver(-1) && op.Version != ver {
			return ErrBadVersion
		}

		if decodeInt64([]byte(s.Get("/zk/cver/"+p))) != 0 {
			panic("how to delete children")
		}

		nextCVer := encodeInt64(decodeInt64([]byte(s.Get(pkey))) + 1)
		s.Put("/zk/cver/"+pp, nextCVer)
		s.Put("/zk/mtime/"+pp, encodeTime())

		s.Del(key)
		s.Del("/zk/ctime/" + p)
		s.Del("/zk/mtime/" + p)
		s.Del("/zk/ver/" + p)
		s.Del("/zk/cver/" + p)
		s.Del("/zk/aver/" + p)
		s.Del("/zk/acl/" + p)

		return nil
	}

	resp, err := v3sync.NewSTMSerializable(z.s.c.Ctx(), z.s.c, applyf)

	switch err {
	case ErrNoNode:
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, 0, &errResp)
	case ErrBadVersion:
		errResp := ErrCode(errBadVersion)
		return z.s.Send(xid, 0, &errResp)
	case nil:
	default:
		return err
	}

	delResp := &DeleteResponse{}
	zxid := ZXid(resp.Header.Revision)
	z.s.w.wait(zxid, p, EventNodeDeleted)
	return z.s.Send(xid, zxid, delResp)
}

func (z *zkEtcd) Exists(xid Xid, op *ExistsRequest) error {
	p := mkPath(op.Path)
	gets := statGets(p)
	txnresp, err := z.s.c.Txn(z.s.c.Ctx()).Then(gets...).Commit()
	if err != nil {
		return err
	}

	exResp := &ExistsResponse{}
	exResp.Stat = statTxn(txnresp)
	zxid := ZXid(txnresp.Header.Revision)
	z.s.w.wait(zxid, p, EventNodeCreated)

	if op.Watch {
		ev := EventNodeDeleted
		if exResp.Stat.Mtime == 0 {
			ev = EventNodeCreated
		}
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  ev,
				State: StateConnected,
				Path:  op.Path,
			}
			z.s.Send(xid, newzxid, wresp)
		}
		z.s.w.watch(zxid, xid, p, ev, f)
	}

	if exResp.Stat.Mtime == 0 {
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, 0, &errResp)
	}

	return z.s.Send(xid, zxid, exResp)
}

func (z *zkEtcd) GetData(xid Xid, op *GetDataRequest) error {
	p := mkPath(op.Path)
	gets := statGets(p)
	txnresp, err := z.s.c.Txn(z.s.c.Ctx()).Then(gets...).Commit()
	if err != nil {
		return err
	}

	datResp := &GetDataResponse{}
	datResp.Stat = statTxn(txnresp)
	if datResp.Stat.Mtime == 0 {
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, 0, &errResp)
	}

	zxid := ZXid(txnresp.Header.Revision)
	z.s.w.wait(zxid, p, EventNodeDataChanged)

	if op.Watch {
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  EventNodeDataChanged,
				State: StateConnected,
				Path:  op.Path,
			}
			z.s.Send(xid, newzxid, wresp)
		}
		z.s.w.watch(zxid, xid, p, EventNodeDataChanged, f)
	}
	datResp.Data = []byte(txnresp.Responses[2].GetResponseRange().Kvs[0].Value)
	return z.s.Send(xid, zxid, datResp)
}

func (z *zkEtcd) SetData(xid Xid, op *SetDataRequest) error {
	p := mkPath(op.Path)
	var statResp etcd.TxnResponse
	applyf := func(s v3sync.STM) error {
		if s.Rev("/zk/ver/"+p) == 0 {
			return ErrNoNode
		}
		currentVersion := Ver(decodeInt64([]byte(s.Get("/zk/ver/" + p))))
		if op.Version != Ver(-1) && op.Version != currentVersion {
			return ErrBadVersion

		}
		s.Put("/zk/key/"+p, string(op.Data))
		s.Put("/zk/ver/"+p, string(encodeInt64(int64(currentVersion+1))))
		s.Put("/zk/mtime/"+p, encodeTime())

		resp, err := z.s.c.Txn(z.s.c.Ctx()).Then(statGets(p)...).Commit()
		if err != nil {
			return err
		}
		statResp = *resp
		return nil
	}
	resp, err := v3sync.NewSTMSerializable(z.s.c.Ctx(), z.s.c, applyf)

	switch err {
	case nil:
	case ErrNoNode:
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, 0, &errResp)
	case ErrBadVersion:
		errResp := ErrCode(errBadVersion)
		return z.s.Send(xid, 0, &errResp)
	default:
		return nil
	}

	sdresp := &SetDataResponse{}
	sdresp.Stat = statTxn(&statResp)
	return z.s.Send(xid, ZXid(resp.Header.Revision), sdresp)
}

func (z *zkEtcd) GetAcl(xid Xid, op *GetAclRequest) error {
	resp := &GetAclResponse{}
	p := mkPath(op.Path)

	gets := []etcd.Op{etcd.OpGet("/zk/acl/" + p)}
	gets = append(gets, statGets(p)...)
	txnresp, err := z.s.c.Txn(z.s.c.Ctx()).Then(gets...).Commit()
	if err != nil {
		return err
	}
	resps := txnresp.Responses
	txnresp.Responses = resps[1:]
	resp.Stat = statTxn(txnresp)
	if resp.Stat.Ctime == 0 {
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, 0, &errResp)
	}
	resp.Acl = decodeACLs(resps[0].GetResponseRange().Kvs[0].Value)
	return z.s.Send(xid, ZXid(txnresp.Header.Revision), resp)
}

func (z *zkEtcd) SetAcl(xid Xid, op *SetAclRequest) error { panic("setAcl") }

func (z *zkEtcd) GetChildren(xid Xid, op *GetChildrenRequest) error {
	p := mkPath(op.Path)
	txnresp, err := z.s.c.Txn(z.s.c.Ctx()).Then(statGets(p)...).Commit()
	if err != nil {
		return err
	}

	s := statTxn(txnresp)
	if len(p) != 2 && s.Ctime == 0 {
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, ZXid(txnresp.Header.Revision), &errResp)
	}

	children := txnresp.Responses[6].GetResponseRange()
	resp := &GetChildrenResponse{}
	for _, kv := range children.Kvs {
		zkkey := strings.Replace(string(kv.Key), getListPfx(p), "", 1)
		resp.Children = append(resp.Children, zkkey)
	}

	return z.s.Send(xid, ZXid(children.Header.Revision), resp)
}

func (z *zkEtcd) Sync(xid Xid, op *SyncRequest) error {
	// linearized read
	resp, err := z.s.c.Get(z.s.c.Ctx(), "/zk/ver/"+mkPath(op.Path))
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		errResp := ErrCode(errNoNode)
		return z.s.Send(xid, 0, &errResp)
	}
	return z.s.Send(xid, ZXid(resp.Header.Revision), &CreateResponse{op.Path})
}

func (z *zkEtcd) Multi(xid Xid, op *MultiRequest) error { panic("multi") }

func (z *zkEtcd) Close(xid Xid, op *CloseRequest) error {
	z.s.Send(xid, 0, &CloseResponse{})
	return ErrConnectionClosed
}

func (z *zkEtcd) SetAuth(xid Xid, op *SetAuthRequest) error { panic("setAuth") }

func (z *zkEtcd) SetWatches(xid Xid, op *SetWatchesRequest) error { panic("setWatches") }

func encodeACLs(acls []ACL) string {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(acls)
	return b.String()
}

func decodeACLs(acls []byte) (ret []ACL) {
	var b bytes.Buffer
	b.Write(acls)
	gob.NewDecoder(&b).Decode(&ret)
	return ret
}

func encodeTime() string {
	return encodeInt64(time.Now().UnixNano() / 1000)
}

func decodeInt64(v []byte) int64 { x, _ := binary.Varint(v); return x }

func encodeInt64(v int64) string {
	b := make([]byte, binary.MaxVarintLen64)
	return string(b[:binary.PutVarint(b, v)])
}
