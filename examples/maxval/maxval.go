package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"gob"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"waffle"
)

type MaxValueVertex struct {
	waffle.VertexBase
	Value int
	Max   int
}

type MaxValueMsg struct {
	waffle.MsgBase
	Value int
}

// Load vertices from generated data file
type MaxValueLoader struct {
	path string
}

func vertexBuilder(id, val string) *MaxValueVertex {
	v := &MaxValueVertex{}
	v.SetVertexId(strings.TrimSpace(id))
	if val, err := strconv.Atoi(strings.TrimSpace(val)); err != nil {
		return nil
	} else {
		v.Value = val
	}
	return v
}

func (l *MaxValueLoader) Load(w *waffle.Worker) (loaded uint64, err error) {
	var file *os.File
	if file, err = os.Open(l.path); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(file)

	var line string
	for {
		if line, err = reader.ReadString('\n'); err != nil {
			break
		}
		split := strings.Split(line, "\t")
		v := vertexBuilder(split[0], split[1])
		if v == nil {
			return loaded, errors.New("bad vertex load")
		}
		for _, val := range split[2:] {
			e := &waffle.EdgeBase{}
			e.SetTarget(strings.TrimSpace(val))
			v.AddOutEdge(e)
		}
		v.SetActive(true)
		w.AddVertex(v)
		loaded++
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// Writes max value to stdout
type MaxValueResultWriter struct {

}

func (rw *MaxValueResultWriter) WriteResults(w *waffle.Worker) error {
	max := 0
	for _, p := range w.Partitions() {
		for _, v := range p.Vertices() {
			mvv := v.(*MaxValueVertex)
			if mvv.Max > max {
				max = mvv.Max
			}
		}
	}
	fmt.Printf("Max value: %d\n", max)
	return nil
}

// Combine to a single max value message
type MaxValueCombiner struct {

}

func (c *MaxValueCombiner) Combine(msgs []waffle.Msg) []waffle.Msg {
	if len(msgs) == 0 {
		return msgs
	}
	maxMsg := msgs[0].(*MaxValueMsg)
	for _, msg := range msgs {
		if msg.(*MaxValueMsg).Value > maxMsg.Value {
			maxMsg = msg.(*MaxValueMsg)
		}
	}
	return []waffle.Msg{maxMsg}
}

// Do work
func (v *MaxValueVertex) Compute(msgs []waffle.Msg) {
	max := 0
	for _, msg := range msgs {
		val := msg.(*MaxValueMsg).Value
		if val > max {
			max = val
		}
	}
	if v.Value > max {
		max = v.Value
	}
	if max > v.Max {
		v.Max = max
		for _, e := range v.OutEdges() {
			v.SendMessageTo(e.Target(), &MaxValueMsg{Value: v.Max})
		}
	}
	v.VoteToHalt()
}

var master bool
var host, port, maddr, loadpath, persistpath string
var minWorkers uint64

func main() {

	flag.BoolVar(&master, "master", false, "node is master")
	flag.StringVar(&maddr, "maddr", "127.0.0.1:50000", "master address")
	flag.StringVar(&port, "port", "50000", "node port")
	flag.StringVar(&host, "host", "127.0.0.1", "node address")
	flag.Uint64Var(&minWorkers, "minWorkers", 6, "min workers")
	flag.StringVar(&loadpath, "loadpath", "testdata/testdata.txt", "data load path")
	flag.StringVar(&persistpath, "persistpath", "persist", "data load path")

	flag.Parse()

	gob.Register(&MaxValueVertex{})
	gob.Register(&MaxValueMsg{})

	if master {
		m := waffle.NewMaster(host, port)

		m.Config.JobId = "maxval-" + time.UTC().String()
		m.Config.MinWorkers = minWorkers
		m.Config.PartitionsPerWorker = 1

		m.SetRpcClient(new(waffle.GobMasterRpcClient))
		m.SetRpcServer(new(waffle.GobMasterRpcServer))
		m.SetCheckpointFn(func(checkpoint uint64) bool {
			return false
		})
		m.Run()
	} else {
		w := waffle.NewWorker(host, port)
		w.Config.MessageThreshold = 1000
		w.Config.VertexThreshold = 1000

		w.Config.MasterHost, w.Config.MasterPort, _ = net.SplitHostPort(maddr)

		w.SetRpcClient(new(waffle.GobWorkerRpcClient))
		w.SetRpcServer(new(waffle.GobWorkerRpcServer))
		w.SetLoader(&MaxValueLoader{path: loadpath})
		w.SetPersister(waffle.NewGobPersister(persistpath))
		w.SetResultWriter(&MaxValueResultWriter{})
		w.AddCombiner(&MaxValueCombiner{})
		w.Run()
	}
}
