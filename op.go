package zetcd

import "fmt"

// use a map

func op2req(op Op) interface{} {
	switch op {
	case opGetChildren2:
		return &GetChildren2Request{}
	case opPing:
		return &PingRequest{}
	case opCreate:
		return &CreateRequest{}
	case opCheck:
		return &CheckVersionRequest{}
	case opSetWatches:
		return &SetWatchesRequest{}
	case opSetData:
		return &SetDataRequest{}
	case opGetData:
		return &GetDataRequest{}
	case opDelete:
		return &DeleteRequest{}
	case opExists:
		return &ExistsRequest{}
	case opGetAcl:
		return &GetAclRequest{}
	case opSetAcl:
		return &SetAclRequest{}
	case opGetChildren:
		return &GetChildrenRequest{}
	case opSync:
		return &SyncRequest{}
	case opMulti:
		return &MultiRequest{}
	case opClose:
		return &CloseRequest{}
	case opSetAuth:
		return &SetAuthRequest{}

	default:
		fmt.Println("unknown opcode ", op)
	}
	return nil
}
