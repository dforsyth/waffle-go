package waffle

import (
	"encoding/gob"
)

type node struct {
	host         string
	port         string
	partitionMap map[uint64]string // partition map (partition id -> worker id)
	partFn       func(string, uint64) uint64
	aggregators  map[string]Aggregator
}

func (n *node) Host() string {
	return n.host
}

func (n *node) Port() string {
	return n.port
}

func (n *node) initNode(host, port string) {
	gob.Register(&LoadTask{})
	gob.Register(&LoadTaskResponse{})
	gob.Register(&DistributeTask{})
	gob.Register(&DistributeTaskResponse{})
	gob.Register(&PrepareTask{})
	gob.Register(&PrepareTaskResponse{})
	gob.Register(&SuperstepTask{})
	gob.Register(&SuperstepTaskResponse{})
	gob.Register(&WriteTask{})
	gob.Register(&WriteTaskResponse{})
	gob.Register(&VertexMsg{})
	gob.Register(&VertexBase{})
	gob.Register(&EdgeBase{})
	gob.Register(&MsgBase{})

	n.host = host
	n.port = port
	n.partFn = func(id string, n uint64) uint64 {
		var sum uint64 = 0
		for _, c := range id {
			sum += uint64(c)
		}
		return sum % n
	}
	n.aggregators = make(map[string]Aggregator)
}

func (n *node) partitionOf(id string) uint64 {
	return n.partFn(id, uint64(len(n.partitionMap)))
}

func (n *node) SetPartitionFn(fn func(string, uint64) uint64) {
	n.partFn = fn
}

func (n *node) AddAggregator(aggr Aggregator) {
	n.aggregators[aggr.Name()] = aggr
}
