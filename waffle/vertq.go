package waffle

import (
	"os"
)

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
	w      *Worker
	thresh uint64
}

func newOutVertexQ(w *Worker, thresh uint64) *OutVertexQ {
	q := &OutVertexQ{
		verts:  make(map[string][]Vertex, 0),
		s:      make(chan byte, 1),
		w:      w,
		thresh: thresh,
	}
	q.s <- 1
	return q
}

func (q *OutVertexQ) addVertex(v Vertex) {
	<-q.s
	pid := q.w.getPartitionOf(v.VertexId())
	wid := q.w.pmap[pid]
	if _, ok := q.verts[wid]; !ok {
		q.verts[wid] = make([]Vertex, 0)
	}
	q.verts[wid] = append(q.verts[wid], v)
	q.s <- 1
}

func (q *OutVertexQ) flush() os.Error {
	<-q.s
	for wid, verts := range q.verts {
		if e := q.sendVertices(wid, verts); e != nil {
			panic(e)
		}
	}
	q.s <- 1
	return nil
}

func (q *OutVertexQ) sendVertices(id string, verts []Vertex) os.Error {
	cl, e := q.w.cl(id)
	if e != nil {
		return e
	}
	var r Resp
	if e = cl.Call("Worker.QueueVertices", verts, &r); e != nil {
		return e
	}
	return nil
}

func (q *OutVertexQ) clear() {
	q.verts = make(map[string][]Vertex)
}
