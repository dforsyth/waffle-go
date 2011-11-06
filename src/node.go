package waffle

import ()

type Node interface {
	InitNode(string, string)
	Run()
	SetVertexPartitionFn(func(string, uint64) uint64) // Drop this setter and expose partFn?
}

type node struct {
	addr         string
	port         string
	partitionMap map[uint64]string // partition map (partition id -> worker id)
	workerMap    map[string]string // worker map (worker id -> worker address)
	partFn       func(string, uint64) uint64
}

func (n *node) Host() string {
	return n.addr
}

func (n *node) Port() string {
	return n.port
}

func (n *node) Workers() map[string]string {
	return n.workerMap
}

func (n *node) InitNode(addr, port string) {
	n.addr = addr
	n.port = port
	n.partFn = func(id string, n uint64) uint64 {
		var sum uint64 = 0
		for _, c := range id {
			sum += uint64(c)
		}
		return sum % n
	}
}

func (n *node) partitionOf(id string) uint64 {
	return n.partFn(id, uint64(len(n.partitionMap)))
}

func (n *node) SetVertexPartitionFn(fn func(string, uint64) uint64) {
	n.partFn = fn
}
