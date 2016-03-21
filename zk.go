package zetcd

import "fmt"

type ZK interface {
	Create(xid Xid, op *CreateRequest) error
	Delete(xid Xid, op *DeleteRequest) error
	Exists(xid Xid, op *ExistsRequest) error
	GetData(xid Xid, op *GetDataRequest) error
	SetData(xid Xid, op *SetDataRequest) error
	GetAcl(xid Xid, op *GetAclRequest) error
	SetAcl(xid Xid, op *SetAclRequest) error
	GetChildren(xid Xid, op *GetChildrenRequest) error
	Sync(xid Xid, op *SyncRequest) error
	Ping(xid Xid, op *PingRequest) error
	GetChildren2(xid Xid, op *GetChildren2Request) error
	// opCheck		= 13
	Multi(xid Xid, op *MultiRequest) error
	Close(xid Xid, op *CloseRequest) error
	SetAuth(xid Xid, op *SetAuthRequest) error
	SetWatches(xid Xid, op *SetWatchesRequest) error
}

func DispatchZK(zk ZK, xid Xid, op interface{}) error {
	switch op := op.(type) {
	case *CreateRequest:
		return zk.Create(xid, op)
	case *DeleteRequest:
		return zk.Delete(xid, op)
	case *GetChildren2Request:
		return zk.GetChildren2(xid, op)
	case *PingRequest:
		return zk.Ping(xid, op)
	case *GetDataRequest:
		return zk.GetData(xid, op)
	case *SetDataRequest:
		return zk.SetData(xid, op)
	case *ExistsRequest:
		return zk.Exists(xid, op)
	case *SyncRequest:
		return zk.Sync(xid, op)
	default:
		fmt.Printf("unexpected type %d %T\n", xid, op)
	}
	return ErrAPIError
}
