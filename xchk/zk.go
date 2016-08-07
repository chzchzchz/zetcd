package xchk

import (
	"bytes"
	"fmt"

	"github.com/chzchzchz/zetcd"
	"github.com/golang/glog"
)

var (
	errStat        = fmt.Errorf("stat mismatch")
	errData        = fmt.Errorf("data mismatch")
	errAcl         = fmt.Errorf("acl mismatch")
	errNumAcl      = fmt.Errorf("acl length mismatch")
	errPath        = fmt.Errorf("path mismatch")
	errErr         = fmt.Errorf("err mismatch")
	errNumChildren = fmt.Errorf("number of children mismatch")
	errChildren    = fmt.Errorf("children paths mismatch")
)

// zkXchk takes incoming ZK requests and forwards them to a remote ZK server
type zkXchk struct {
	s *session

	cZK zetcd.ZK
	oZK zetcd.ZK
}

func newZK(s *session, cZKf, oZKf zetcd.ZKFunc) (*zkXchk, error) {
	cZK, cerr := cZKf(s.candidate)
	if cerr != nil {
		return nil, cerr
	}
	oZK, oerr := oZKf(s.oracle)
	if oerr != nil {
		cZK.CloseZK()
		return nil, oerr
	}
	return &zkXchk{s, cZK, oZK}, nil
}

func (xchk *zkXchk) CloseZK() error { return nil }

func (xchk *zkXchk) Create(xid zetcd.Xid, op *zetcd.CreateRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Create(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Create(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.CreateResponse), or.Resp.(*zetcd.CreateResponse)
	if crr.Path != orr.Path {
		err = errPath
	}
	return or
}

func (xchk *zkXchk) Delete(xid zetcd.Xid, op *zetcd.DeleteRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Delete(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Delete(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	return or
}

func (xchk *zkXchk) Exists(xid zetcd.Xid, op *zetcd.ExistsRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Exists(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Exists(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.ExistsResponse), or.Resp.(*zetcd.ExistsResponse)

	if crr.Stat != orr.Stat {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) GetData(xid zetcd.Xid, op *zetcd.GetDataRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetData(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetData(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetDataResponse), or.Resp.(*zetcd.GetDataResponse)

	if bytes.Compare(crr.Data, orr.Data) != 0 {
		err = errData
	}
	if crr.Stat != orr.Stat {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) SetData(xid zetcd.Xid, op *zetcd.SetDataRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetData(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetData(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.SetDataResponse), or.Resp.(*zetcd.SetDataResponse)

	if crr.Stat != orr.Stat {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) GetAcl(xid zetcd.Xid, op *zetcd.GetAclRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetAcl(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetAcl(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetAclResponse), or.Resp.(*zetcd.GetAclResponse)

	if len(crr.Acl) != len(orr.Acl) {
		err = errNumAcl
		return or
	}

	for i := range crr.Acl {
		if crr.Acl[i] != orr.Acl[i] {
			err = errAcl
			return or
		}
	}

	if crr.Stat != orr.Stat {
		err = errStat
	}

	return or
}

func (xchk *zkXchk) SetAcl(xid zetcd.Xid, op *zetcd.SetAclRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetAcl(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetAcl(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.SetAclResponse), or.Resp.(*zetcd.SetAclResponse)

	if crr.Stat != orr.Stat {
		err = errStat
	}

	return or
}

func (xchk *zkXchk) GetChildren(xid zetcd.Xid, op *zetcd.GetChildrenRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetChildren(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetChildren(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetChildrenResponse), or.Resp.(*zetcd.GetChildrenResponse)

	if len(crr.Children) != len(orr.Children) {
		err = errNumChildren
		return or
	}
	for i := range crr.Children {
		if crr.Children[i] != orr.Children[i] {
			err = errChildren
			return or
		}
	}

	return or
}

func (xchk *zkXchk) Sync(xid zetcd.Xid, op *zetcd.SyncRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Sync(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Sync(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.SyncResponse), or.Resp.(*zetcd.SyncResponse)
	if crr.Path != orr.Path {
		err = errPath
	}
	return or
}

func (xchk *zkXchk) Ping(xid zetcd.Xid, op *zetcd.PingRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Ping(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Ping(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	return or
}

func (xchk *zkXchk) GetChildren2(xid zetcd.Xid, op *zetcd.GetChildren2Request) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetChildren2(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetChildren2(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetChildren2Response), or.Resp.(*zetcd.GetChildren2Response)
	if len(crr.Children) != len(orr.Children) {
		err = errNumChildren
		return or
	}
	for i := range crr.Children {
		if crr.Children[i] != orr.Children[i] {
			err = errChildren
			return or
		}
	}
	if crr.Stat != orr.Stat {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) Multi(xid zetcd.Xid, op *zetcd.MultiRequest) zetcd.ZKResponse { panic("wut") }

func (xchk *zkXchk) Close(xid zetcd.Xid, op *zetcd.CloseRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Close(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Close(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	return or
}

func (xchk *zkXchk) SetAuth(xid zetcd.Xid, op *zetcd.SetAuthRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetAuth(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetAuth(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	return or
}

func (xchk *zkXchk) SetWatches(xid zetcd.Xid, op *zetcd.SetWatchesRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetWatches(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetWatches(xid, op) }
	cr, or, err := xchkResp(cf, of)
	defer func() { reportErr(cr, or, err) }()
	return or
}

type zkfunc func() zetcd.ZKResponse

func xchkErr(cresp, oresp zetcd.ZKResponse) bool {
	if cresp.Err != nil || oresp.Err != nil {
		return false
	}
	if cresp.Hdr == nil || oresp.Hdr == nil {
		return false
	}
	if cresp.Hdr.Err != oresp.Hdr.Err {
		return false
	}
	return true
}

func xchkResp(cf, of zkfunc) (cresp zetcd.ZKResponse, oresp zetcd.ZKResponse, err error) {
	cch, och := make(chan zetcd.ZKResponse, 1), make(chan zetcd.ZKResponse, 1)
	go func() { cch <- cf() }()
	go func() { och <- of() }()
	cresp, oresp = <-cch, <-och
	if !xchkErr(cresp, oresp) {
		err = errErr
		return
	}
	return
}

func reportErr(cr, or zetcd.ZKResponse, err error) {
	if err == nil {
		return
	}
	switch {
	case cr.Resp != nil && or.Resp != nil:
		glog.Warningf("xchk failed (%v)\ncandiate: %+v\noracle: %+v\n", err, cr.Resp, or.Resp)
	case cr.Hdr != nil && or.Hdr != nil:
		glog.Warningf("xchk failed (%v)\ncandidate: %+v\noracle: %+v\n", err, cr.Hdr, or.Hdr)
	default:
		glog.Warningf("xchk failed (%v)\ncandidate: %+v\noracle: %+v", err, cr, or)
	}
}
