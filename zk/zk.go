package zk

import (
	"fmt"

	"github.com/chzchzchz/zetcd"
)

// zkZK takes incoming ZK requests and forwards them to a remote ZK server
type zkZK struct{ s *session }

func newZK(s zetcd.Session) (*zkZK, error) {
	ss, ok := s.Backing().(*session)
	if !ok {
		return nil, fmt.Errorf("unexpected session type %t", s)
	}
	return &zkZK{ss}, nil
}

func (zz *zkZK) CloseZK() error {
	zz.s.Close()
	return nil
}

func (zz *zkZK) Create(xid zetcd.Xid, op *zetcd.CreateRequest) error {
	return zz.s.zkc.Send(xid, op)
}

func (zz *zkZK) Delete(xid zetcd.Xid, op *zetcd.DeleteRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) Exists(xid zetcd.Xid, op *zetcd.ExistsRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) GetData(xid zetcd.Xid, op *zetcd.GetDataRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) SetData(xid zetcd.Xid, op *zetcd.SetDataRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) GetAcl(xid zetcd.Xid, op *zetcd.GetAclRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) SetAcl(xid zetcd.Xid, op *zetcd.SetAclRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) GetChildren(xid zetcd.Xid, op *zetcd.GetChildrenRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) Sync(xid zetcd.Xid, op *zetcd.SyncRequest) error { return zz.s.zkc.Send(xid, op) }
func (zz *zkZK) Ping(xid zetcd.Xid, op *zetcd.PingRequest) error { return zz.s.zkc.Send(xid, op) }

func (zz *zkZK) GetChildren2(xid zetcd.Xid, op *zetcd.GetChildren2Request) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) Multi(xid zetcd.Xid, op *zetcd.MultiRequest) error { return zz.s.zkc.Send(xid, op) }
func (zz *zkZK) Close(xid zetcd.Xid, op *zetcd.CloseRequest) error { return zz.s.zkc.Send(xid, op) }
func (zz *zkZK) SetAuth(xid zetcd.Xid, op *zetcd.SetAuthRequest) error {
	return zz.s.zkc.Send(xid, op)
}
func (zz *zkZK) SetWatches(xid zetcd.Xid, op *zetcd.SetWatchesRequest) error {
	return zz.s.zkc.Send(xid, op)
}
