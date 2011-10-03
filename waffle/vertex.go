package waffle

type Vertex interface {
	VertexId() string
	SetVertexId(string)
	Compute([]Msg)
	AddOutEdgeTo(string, Edge)
	AddOutEdge(Edge)
	OutEdges() []Edge
	SetPartition(*Partition) // have to leave this exported for partition assignment
	VoteToHalt()
	SetActive(bool)
	IsActive() bool
}

type VertexBase struct {
	Id     string
	Edges  []Edge
	Active bool
	// XXX This is an interface because RPC breaks if it's a *Partition even
	// though it isn't exported.  Apparently the gob encoder tries to get to
	// it, and the rpc types in Worker break the encoder.  Investigate later.
	part interface{}
}

func (v *VertexBase) VertexId() string {
	return v.Id
}

func (v *VertexBase) SetVertexId(id string) {
	v.Id = id
}

func (v *VertexBase) AddOutEdge(e Edge) {
	if v.Edges == nil {
		v.Edges = make([]Edge, 0)
	}
	v.Edges = append(v.Edges, e)
}

func (v *VertexBase) OutEdges() []Edge {
	if v.Edges == nil {
		return make([]Edge, 0)
	}
	return v.Edges
}

func (v *VertexBase) removeOutEdge(target string) {
	if v.Edges == nil {
		return
	}
	for i, e := range v.Edges {
		if e.Target() == target {
			v.Edges = append(v.Edges[:i], v.Edges[i+1:]...)
		}
	}
}

func (v *VertexBase) RemoveOutEdgeFrom(source, target string) {
	msg := &RemoveEdgeMsg{TargetId: target}
	msg.SetDestVertId(source)
	v.part.(*Partition).worker.outq.addMsg(msg)
}

func (v *VertexBase) RemoveVertex(id string) {
	msg := &RemoveVertexMsg{}
	msg.SetDestVertId(id)
	v.part.(*Partition).worker.outq.addMsg(msg)
}

func (v *VertexBase) AddOutEdgeTo(source string, e Edge) {
	msg := &AddEdgeMsg{E: e}
	msg.SetDestVertId(source)
	v.part.(*Partition).worker.outq.addMsg(msg)
}

func (v *VertexBase) AddVertex(vert Vertex) {
	msg := &AddVertexMsg{V: vert}
	msg.SetDestVertId(v.VertexId())
	v.part.(*Partition).worker.outq.addMsg(msg)
}

func (v *VertexBase) Superstep() uint64 {
	return v.part.(*Partition).worker.superstep
}

func (v *VertexBase) SendMessageTo(dest string, msg Msg) {
	msg.SetDestVertId(dest)
	v.part.(*Partition).worker.outq.addMsg(msg)
}

func (v *VertexBase) Worker() *Worker {
	return v.part.(*Partition).worker
}

func (v *VertexBase) Partition() *Partition {
	return v.part.(*Partition)
}

func (v *VertexBase) SetPartition(p *Partition) {
	v.part = p
}

func (v *VertexBase) VoteToHalt() {
	v.Active = false
}

func (v *VertexBase) SetActive(active bool) {
	v.Active = active
}

func (v *VertexBase) IsActive() bool {
	return v.Active
}

func (v *VertexBase) NumVertices() uint64 {
	return v.part.(*Partition).worker.jobStats.numVertices
}
