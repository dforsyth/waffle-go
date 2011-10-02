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
	s      chan interface{}
}

func (n *node) Addr() string {
	return n.addr
}

func (n *node) Port() string {
	return n.port
}

func dumbHashFn(id string) uint64 {
	var sum uint64 = 0
	for _, c := range id {
		sum += uint64(c)
	}
	return sum
}

func (n *node) getPartitionOf(id string) uint64 {
	return n.partFn(id) % uint64(len(n.pmap))
}

func (n *node) InitNode(addr, port string) {
	n.addr = addr
	n.port = port
	n.cls = make(map[string]*rpc.Client)
	n.partFn = dumbHashFn
	n.s = make(chan interface{}, 1)
	n.s <- 1
}

func (n *node) cl(wid string) (*rpc.Client, os.Error) {
	<-n.s
	defer func() { n.s <- 1 }()
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
