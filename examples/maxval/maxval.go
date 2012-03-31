package main

import (
	"bufio"
	"encoding/gob"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"waffle"
)

type MVJob struct {
}

func (j *MVJob) Id() string {
	return "MVJob"
}

func (j *MVJob) LoadPaths() (paths []string) {
	files, err := ioutil.ReadDir("./testdata")
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".data") {
			paths = append(paths, file.Name())
		}
	}
	/*
		log.Println("Files to load:")
		for _, name := range paths {
			log.Printf("\t->%s", name)
		}
	*/
	return
}

func vertexBuilder(id, val string) *MVVertex {
	v := &MVVertex{}
	v.Vid = strings.TrimSpace(id)
	v.Value, _ = strconv.Atoi(strings.TrimSpace(val))
	return v
}

func (j *MVJob) Load(p string) ([]waffle.Vertex, []waffle.Edge, error) {
	log.Printf("loading %s", p)
	// do the load
	var file *os.File
	var err error
	if file, err = os.Open(path.Join("./testdata", p)); err != nil {
		return nil, nil, err
	}
	reader := bufio.NewReader(file)

	var line string
	var verts []waffle.Vertex
	var edges []waffle.Edge
	log.Printf("%s: going into load loop", p)
	for {
		if line, err = reader.ReadString('\n'); err != nil {
			if err != io.EOF {
				return nil, nil, err
			} else {
				break
			}
		}
		if len(line) <= 0 || line[0] == '#' {
			// comment line
			log.Printf("(%s) skipping line: \"%s\"", p, line)
			continue
		}
		split := strings.Split(line, "\t")
		v := vertexBuilder(split[0], split[1])
		if v == nil {
			return nil, nil, errors.New("bad vertex load")
		}
		for _, val := range split[2:] {
			e := &MVEdge{
				Esrc: v.Id(),
				Edst: strings.TrimSpace(val),
			}
			edges = append(edges, e)
		}
		v.Vactive = true
		verts = append(verts, v)
	}
	log.Printf("(%s) loaded %d verts, %d edges", p, len(verts), len(edges))
	return verts, edges, nil
}

func (j *MVJob) Checkpoint(step int) bool {
	return true
}

func (j *MVJob) Persist(g *waffle.Graph) error {
	return nil
}

func (j *MVJob) Write(g *waffle.Graph) error {
	m := -1
	for _, v := range g.Vertices() {
		v := v.(*MVVertex)
		if v.Max > m {
			m = v.Max
		}
	}
	log.Printf("max is %d", m)
	return nil
}

type MVVertex struct {
	Vid     string
	Vactive bool
	Value   int
	Max     int
}

func (v *MVVertex) Id() string {
	return v.Vid
}

func (v *MVVertex) Compute(g *waffle.Graph, msgs []waffle.Message) {
	max := 0
	for _, msg := range msgs {
		val := msg.(*MVMessage).Value
		if val > max {
			max = val
		}
	}
	if v.Value > max {
		max = v.Value
	}
	if max > v.Max {
		v.Max = max
		for _, e := range g.Edges(v.Id()) {
			g.SendMessage(&MVMessage{Value: v.Max, Dest: e.Destination()})
		}
	}
	v.Vactive = false
}

func (v *MVVertex) Active() bool {
	return v.Vactive
}

type MVEdge struct {
	Esrc, Edst string
}

func (e *MVEdge) Source() string {
	return e.Esrc
}

func (e *MVEdge) Destination() string {
	return e.Edst
}

type MVMessage struct {
	Value int
	Dest  string
}

func (m *MVMessage) Destination() string {
	return m.Dest
}

func main() {
	gob.Register(&MVVertex{})
	gob.Register(&MVMessage{})
	gob.Register(&MVEdge{})

	workers := flag.Int("workers", 1, "number of workers")
	nodeId := flag.String("nodeId", "node", "node identifier")
	zkServers := flag.String("zkServers", "", "zk servers to connect to")
	rpcHost := flag.String("rpcHost", "localhost", "rpc host for this worker")
	rpcPort := flag.String("rpcPort", "6000", "rpc port for this worker")
	flag.Parse()

	config := &waffle.Config{
		InitialWorkers: *workers,
		NodeId:         *nodeId,
		ZKServers:      *zkServers,
		RPCHost:        *rpcHost,
		RPCPort:        *rpcPort,
	}
	waffle.Run(config, &MVJob{})
}
