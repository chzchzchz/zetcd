package xchk

import (
	"fmt"

	"github.com/chzchzchz/zetcd"
)

// NewAuth takes a candidate AuthFunc and an oracle AuthFunc
func NewAuth(cAuth, oAuth zetcd.AuthFunc) zetcd.AuthFunc {
	sp := newSessionPool()
	return func(zka zetcd.AuthConn) (zetcd.Session, error) {
		return Auth(sp, zka, cAuth, oAuth)
	}
}

// NewZK takes a candidate ZKFunc and an oracle ZKFunc, returning a cross checker.
func NewZK(cZK, oZK zetcd.ZKFunc) zetcd.ZKFunc {
	return func(s zetcd.Session) (zetcd.ZK, error) {
		ss, ok := s.(*session)
		if !ok {
			return nil, fmt.Errorf("expected xchk.session")
		}
		return newZK(ss, cZK, oZK)
	}
}
