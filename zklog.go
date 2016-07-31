package zetcd

import (
	"github.com/golang/glog"
)

type zkLog struct{ zk ZK }

func NewZKLog(zk ZK) ZK {
	return &zkLog{zk}
}

func (zl *zkLog) Create(xid Xid, op *CreateRequest) error {
	glog.V(7).Infof("Create(%v,%+v)", xid, *op)
	return zl.zk.Create(xid, op)
}

func (zl *zkLog) Delete(xid Xid, op *DeleteRequest) error {
	glog.V(7).Infof("Delete(%v,%+v)", xid, *op)
	return zl.zk.Delete(xid, op)
}

func (zl *zkLog) Exists(xid Xid, op *ExistsRequest) error {
	glog.V(7).Infof("Exists(%v,%+v)", xid, *op)
	return zl.zk.Exists(xid, op)
}

func (zl *zkLog) GetData(xid Xid, op *GetDataRequest) error {
	glog.V(7).Infof("GetData(%v,%+v)", xid, *op)
	return zl.zk.GetData(xid, op)
}

func (zl *zkLog) SetData(xid Xid, op *SetDataRequest) error {
	glog.V(7).Infof("SetData(%v,%+v)", xid, *op)
	return zl.zk.SetData(xid, op)
}

func (zl *zkLog) GetAcl(xid Xid, op *GetAclRequest) error {
	glog.V(7).Infof("GetAcl(%v,%+v)", xid, *op)
	return zl.zk.GetAcl(xid, op)
}

func (zl *zkLog) SetAcl(xid Xid, op *SetAclRequest) error {
	glog.V(7).Infof("SetAcl(%v,%+v)", xid, *op)
	return zl.zk.SetAcl(xid, op)
}

func (zl *zkLog) GetChildren(xid Xid, op *GetChildrenRequest) error {
	glog.V(7).Infof("GetChildren(%v,%+v)", xid, *op)
	return zl.zk.GetChildren(xid, op)
}

func (zl *zkLog) Sync(xid Xid, op *SyncRequest) error {
	glog.V(7).Infof("Sync(%v,%+v)", xid, *op)
	return zl.zk.Sync(xid, op)
}

func (zl *zkLog) Ping(xid Xid, op *PingRequest) error {
	glog.V(7).Infof("Ping(%v,%+v)", xid, *op)
	return zl.zk.Ping(xid, op)
}

func (zl *zkLog) GetChildren2(xid Xid, op *GetChildren2Request) error {
	glog.V(7).Infof("GetChildren2(%v,%+v)", xid, *op)
	return zl.zk.GetChildren2(xid, op)
}

func (zl *zkLog) Multi(xid Xid, op *MultiRequest) error {
	glog.V(7).Infof("Multi(%v,%+v)", xid, *op)
	return zl.zk.Multi(xid, op)
}

func (zl *zkLog) Close(xid Xid, op *CloseRequest) error {
	glog.V(7).Infof("Close(%v,%+v)", xid, *op)
	return zl.zk.Close(xid, op)
}

func (zl *zkLog) SetAuth(xid Xid, op *SetAuthRequest) error {
	glog.V(7).Infof("SetAuth(%v,%+v)", xid, *op)
	return zl.zk.SetAuth(xid, op)
}

func (zl *zkLog) SetWatches(xid Xid, op *SetWatchesRequest) error {
	glog.V(7).Infof("SetWatches(%v,%+v)", xid, *op)
	return zl.zk.SetWatches(xid, op)
}

func (zl *zkLog) CloseZK() error { return zl.zk.CloseZK() }
