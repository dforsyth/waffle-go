package waffle

import (
	"log"
)

type stepStat struct {
	step         int
	active, msgs int
	aggr         map[string]interface{}
}

func (s *stepStat) reset() {
	s.step = 0
	s.active = 0
	s.msgs = 0
	s.aggr = make(map[string]interface{})
}

type Vertex interface {
	Id() string
	Compute(*Graph, []Message)
	Active() bool
}

type Edge interface {
	Source() string
	Destination() string
}

type Message interface {
	Destination() string
}

type Graph struct {
	kills map[string]chan byte
	job   Job

	state       int32
	partitionId int

	// need to point back to the coordinator so we can send things
	coord *Coordinator

	v map[string]Vertex
	e map[string][]Edge
	m map[string][]Message

	// information about the last step
	stat       *stepStat
	globalStat *stepStat
}

func newGraph(j Job, c *Coordinator) *Graph {
	return &Graph{
		v:          make(map[string]Vertex),
		e:          make(map[string][]Edge),
		m:          make(map[string][]Message),
		job:        j,
		coord:      c,
		stat:       &stepStat{},
		globalStat: &stepStat{},
	}
}

func (g *Graph) setStepStats(active, msgs int, aggr map[string]interface{}) {
	g.globalStat.active = active
	g.globalStat.msgs = msgs
	g.globalStat.aggr = aggr
}

func (g *Graph) Load(path string) {
	verticies, edges, err := g.job.Load(path)
	if err != nil {
		panic(err)
	}

	log.Printf("adding verts from %s", path)
	for _, v := range verticies {
		g.addVertex(v)
	}
	log.Printf("adding edges from %s", path)
	for _, e := range edges {
		g.addEdge(e)
	}
	log.Printf("done adding verts and edges from %s", path)
}

func (g *Graph) sendVertex(v Vertex, p int) error {
	return g.coord.sendVertex(v, p)
}

func (g *Graph) addVertex(v Vertex) {
	if p := g.determinePartition(v.Id()); p != g.partitionId {
		if e := g.sendVertex(v, p); e != nil {
			panic(e)
		}
		return
	}
	g.v[v.Id()] = v
}

func (g *Graph) Verticies() map[string]Vertex {
	return g.v
}

func (g *Graph) Edges(id string) []Edge {
	return g.e[id]
}

func (g *Graph) sendEdge(e Edge, p int) error {
	return g.coord.sendEdge(e, p)
}

func (g *Graph) addEdge(e Edge) {
	if p := g.determinePartition(e.Source()); p != g.partitionId {
		if e := g.sendEdge(e, p); e != nil {
			panic(e)
		}
		return
	}
	g.e[e.Source()] = append(g.e[e.Source()], e)
}

func (g *Graph) sendMessage(m Message, p int) error {
	return g.coord.sendMessage(m, p)
}

func (g *Graph) addMessage(m Message) {
	if p := g.determinePartition(m.Destination()); p != g.partitionId {
		if e := g.sendMessage(m, p); e != nil {
			panic(e)
		}
		return
	}
	g.m[m.Destination()] = append(g.m[m.Destination()], m)
}

// TODO: implement
func (g *Graph) determinePartition(id string) int {
	sum := 0
	for _, c := range id {
		sum += int(c)
	}
	return sum % g.coord.workers.Len()
}

// this can only happen during compute()
func (g *Graph) SendMessage(msg Message) {
	// TODO: send stuff
	g.addMessage(msg)
	g.stat.msgs++
}

func (g *Graph) Superstep() int {
	return g.stat.step
}

func (g *Graph) runSuperstep(step int) (int, int, map[string]interface{}) {
	if step != g.globalStat.step+1 {
		panic("bad step")
	}

	if g.job.Checkpoint(step) {
		if err := g.job.Persist(); err != nil {
			panic(err)
		}
	}

	g.stat.step = step
	g.stat.active = 0
	g.stat.msgs = 0
	g.stat.aggr = make(map[string]interface{})

	log.Printf("ready to compute for step %d", step)
	g.compute()
	log.Printf("done with computation for step %d", step)

	return g.stat.active, g.stat.msgs, g.stat.aggr
}

func (g *Graph) compute() {
	log.Printf("computing for %d verts", len(g.v))
	i := 0
	for _, v := range g.v {
		if msgs, ok := g.m[v.Id()]; ok || v.Active() {
			if msgs == nil {
				msgs = make([]Message, 0)
			}
			v.Compute(g, msgs)
		}
		if v.Active() {
			g.stat.active++
		}
		i++
		if i%100 == 0 {
			log.Printf("have computed %d verts", i)
		}
	}
}

func (g *Graph) Write() error {
	g.job.Write(g)
	return nil
}
