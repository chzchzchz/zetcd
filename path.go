package zetcd

import (
	"path"
)

func mkPath(zkPath string) string {
	p := path.Clean(zkPath)
	if p[0] != '/' {
		p = "/" + p
	}
	depth := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			depth++
		}
	}
	return string(append([]byte{byte(depth)}, []byte(p)...))
}

func incPath(zetcdPath string) string {
	b := []byte(zetcdPath)
	b[0]++
	return string(b)
}

func getListPfx(p string) string {
	pfx := "/zk/ver/"
	if len(p) != 2 {
		// /abc => 1 => listing dir needs search on p[0] = 2
		searchP := string([]byte{p[0] + 1}) + p[1:]
		return pfx + searchP + "/"
	}
	return pfx + p
}
