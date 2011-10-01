package waffle

type Vertex interface {
	VertexId() string
	SetVertexId(string)
	Compute([]Msg)
	AddOutEdge(e Edge)
	Partition() *Partition
	SetPartition(*Partition)
	VoteToHalt()
	SetActive(bool)
	IsActive() bool
}

type VertexBase struct {
	Id       string
	OutEdges []Edge
	Active   bool
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
	if v.OutEdges == nil {
		v.OutEdges = make([]Edge, 0)
	}
	v.OutEdges = append(v.OutEdges, e)
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
