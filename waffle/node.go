package waffle

import (
	"os"
	"rpc"
)

type Node interface {
	InitNode(string, string)
	Run()
	SetVertexPartitionFn(func(string) uint64)
	getPartitionOf(string) uint64
}

type node struct {
	addr   string
	port   string
	pmap   map[uint64]string      // partition map (partition id -> worker id)
	wmap   map[string]string      // worker map (worker id -> worker address)
	cls    map[string]*rpc.Client // client map (worker id -> rpc client)
	partFn func(string) uint64
}

func dumbHashFn(id string) uint64 {
	var sum uint64 = 0
	for _, c := range id {
		sum += uint64(c)
	}
	return sum // % uint64(len(n.pmap))
}

func (n *node) getPartitionOf(id string) uint64 {
	return n.partFn(id)
}

func (n *node) InitNode(addr, port string) {
	n.addr = addr
	n.port = port
	n.cls = make(map[string]*rpc.Client)
	n.partFn = dumbHashFn
}

func (n *node) cl(wid string) (*rpc.Client, os.Error) {
	if n.cls == nil {
		n.cls = make(map[string]*rpc.Client)
	}
	if _, ok := n.cls[wid]; !ok {
		cl, err := rpc.DialHTTP("tcp", n.wmap[wid])
		if err != nil {
			return nil, err
		}
		n.cls[wid] = cl
	}
	return n.cls[wid], nil
}

func (n *node) SetVertexPartitionFn(fn func(string) uint64) {
	n.partFn = fn
}
