package waffle

type Partition struct {
	id     uint64
	verts  map[string]Vertex
	worker *Worker
}

func NewPartition(id uint64, w *Worker) *Partition {
	return &Partition{
		id:     id,
		verts:  make(map[string]Vertex),
		worker: w,
	}
}

func (p *Partition) PartitionId() uint64 {
	return p.id
}

func (p *Partition) addVertex(v Vertex) {
	p.verts[v.VertexId()] = v
	v.SetPartition(p)
}

func (p *Partition) Vertices() map[string]Vertex {
	return p.verts
}

func (p *Partition) compute() error {
	// TODO Handle mutations
	for _, v := range p.verts {
		msgs := p.worker.msgs.msgs(v.VertexId())
		if v.IsActive() == false && msgs != nil && len(msgs) > 0 {
			v.SetActive(true)
		}
		if v.IsActive() {
			v.Compute(p.worker.msgs.msgs(v.VertexId()))
		}
	}
	return nil
}

func (p *Partition) numActiveVertices() uint64 {
	var sum uint64 = 0
	for _, v := range p.verts {
		if v.IsActive() {
			sum++
		}
	}
	return sum
}

func (p *Partition) numVertices() uint64 {
	return uint64(len(p.verts))
}
