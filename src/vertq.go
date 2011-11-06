package waffle

import "sync"

type VertexQ interface {
	addVertex(v Vertex)
}

type InVertexQ struct {
	verts []Vertex
	s     chan byte
}

func newInVertexQ() *InVertexQ {
	q := &InVertexQ{
		verts: make([]Vertex, 0),
		s:     make(chan byte, 1),
	}
	q.s <- 1
	return q
}

func (q *InVertexQ) addVertex(v Vertex) {
	<-q.s
	q.verts = append(q.verts, v)
	q.s <- 1
}

func (q *InVertexQ) addVertices(v []Vertex) {
	<-q.s
	q.verts = append(q.verts, v...)
	q.s <- 1
}

func (q *InVertexQ) clear() {
	q.verts = make([]Vertex, 0)
}

type OutVertexQ struct {
	verts  map[string][]Vertex
	s      chan byte
	worker *Worker
	thresh int64
	wait   sync.WaitGroup
}

func newOutVertexQ(w *Worker, thresh int64) *OutVertexQ {
	q := &OutVertexQ{
		verts:  make(map[string][]Vertex, 0),
		s:      make(chan byte, 1),
		worker: w,
		thresh: thresh,
	}
	q.s <- 1
	return q
}

func (q *OutVertexQ) addVertex(v Vertex) {
	<-q.s
	pid := q.worker.getPartitionOf(v.VertexId())
	wid := q.worker.partitionMap[pid]
	if _, ok := q.verts[wid]; !ok {
		q.verts[wid] = make([]Vertex, 0)
	}
	q.verts[wid] = append(q.verts[wid], v)
	if int64(len(q.verts[wid])) > q.thresh {
		verts := q.verts[wid]
		delete(q.verts, wid)
		q.sendVerticesAsync(wid, verts)
	}
	q.s <- 1
}

func (q *OutVertexQ) flush() error {
	<-q.s
	for wid, verts := range q.verts {
		if e := q.sendVertices(wid, verts); e != nil {
			panic(e)
		}
		delete(q.verts, wid)
	}
	q.s <- 1
	return nil
}

func (q *OutVertexQ) sendVerticesAsync(id string, verts []Vertex) chan interface{} {
	ch := make(chan interface{})
	q.wait.Add(1)
	go func() {
		if e := q.sendVertices(id, verts); e != nil {
			// passing back the error this way doesn't give us much to handle it with,
			// we're going to need to be more descriptive at some point
			ch <- e
		}
		q.wait.Done()
	}()
	return ch
}

func (q *OutVertexQ) sendVertices(id string, verts []Vertex) error {

	if id == q.worker.WorkerId() {
		q.worker.vinq.addVertices(verts)
	} else {
		q.worker.rpcClient.SendVertices(id, verts)
	}
	return nil
}

func (q *OutVertexQ) clear() {
	q.verts = make(map[string][]Vertex)
}
