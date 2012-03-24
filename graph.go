package waffle

import ()

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
	coord *coordinator

	v map[string]Vertex
	e map[string][]Edge
	m map[string][]Message

	// information about the last step
	stat       *stepStat
	globalStat *stepStat
}

func (g *Graph) setStepStats(active, msgs int, aggr map[string]interface{}) {
	g.globalStat.active = active
	g.globalStat.msgs = msgs
	g.globalStat.aggr = aggr
}

/*
func (g *Graph) createServers() {
	ctx, _ := gozmq.NewContext()

	var sock *gozmq.Socket
	sock, _ = ctx.NewSocket(gozmq.REP)
	sock.Bind("tcp://localhost:" + strconv.Itoa(vPort))
	g.kills["vert"] = startServer(sock, func(msg []byte) {
		var v Vertex
		json.Unmarshal(msg, &v)
	})
	port++
	sock, _ = ctx.NewSocket(gozmq.REP)
	sock.Bind("tcp://localhost:" + strconv.Itoa(ePort))
	g.kills["edge"] = startServer(sock, func(msg []byte) {
		var e Edge
		json.Unmarshal(msg, &e)
	})
	port++
	sock, _ = ctx.NewSocket(gozmq.REP)
	sock.Bind("tcp://localhost:" + strconv.Itoa(mPort))
	g.kills["msg"] = startServer(sock, func(msg []byte) {
		var m Message
		json.Unmarshal(msg, &m)
	})
}

func (g *Graph) startServer(sock *gozmq.Socket, onRecv func([]byte)) chan byte {
	kill := make(chan byte)
	go func() {
		select {
		case <-kill:
			return
		default:
		}

		msg, err := gozmq.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}

		onRecv(msg)
	}()
	return kill
}
*/

func (g *Graph) Load(path string) {
	verticies, edges, err := g.job.Load(path)
	if err != nil {
		panic(err)
	}

	for _, v := range verticies {
		g.addVertex(v)
	}
	for _, e := range edges {
		g.addEdge(e)
	}
}

func (g *Graph) sendVertex(v Vertex, p int) error {
	g.coord.sendVertex(v, p)
	return nil
}

func (g *Graph) addVertex(v Vertex) {
	if p := g.determinePartition(v.Id()); p != g.partitionId {
		g.sendVertex(v, p)
	}
	g.v[v.Id()] = v
}

func (g *Graph) sendEdge(e Edge, p int) error {
	g.coord.sendEdge(e, p)
	return nil
}

func (g *Graph) addEdge(e Edge) {
	if p := g.determinePartition(e.Source()); p != g.partitionId {
		g.sendEdge(e, p)
	}
	g.e[e.Source()] = append(g.e[e.Source()], e)
}

func (g *Graph) sendMessage(m Message, p int) error {
	g.coord.sendMessage(m, p)
	return nil
}

func (g *Graph) addMessage(m Message) {
	if p := g.determinePartition(m.Destination()); p != g.partitionId {
		g.sendMessage(m, p)
	}
	g.m[m.Destination()] = append(g.m[m.Destination()], m)
}

// TODO: implement
func (g *Graph) determinePartition(id string) int {
	return 0
}

// this can only happen during compute()
func (g *Graph) SendMessageTo(dest string, msg Message) {
	// TODO: send stuff
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

	g.compute()

	return g.stat.active, g.stat.msgs, g.stat.aggr
}

func (g *Graph) compute() {
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
	}
}

func (g *Graph) Write() error {
	return nil
}
