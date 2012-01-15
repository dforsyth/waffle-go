package main

import (
	"batter"
	"bufio"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
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
	basePath string
}

func filesToLoad(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".data") {
			continue
		}
		paths = append(paths, file.Name())
	}
	log.Println("files to load:")
	for _, path := range paths {
		log.Printf("->%s", path)
	}
	return paths, nil
}

func (l *MaxValueLoader) AssignLoad(workers []string, loadPaths []string) map[string][]string {
	var files []string
	for _, path := range loadPaths {
		paths, err := filesToLoad(path)
		if err != nil {
			panic(err)
		}
		files = append(files, paths...)
	}

	assign := make(map[string][]string)
	// XXX ghetto for testing
	for _, hostPort := range workers {
		assign[hostPort] = files
		for _, path := range assign[hostPort] {
			log.Printf("assigned loading: %s -> %s", path, hostPort)
		}
		break
	}

	return assign
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

func (l *MaxValueLoader) Load(w *waffle.Worker, filePath string) (loaded uint64, err error) {
	var file *os.File
	if file, err = os.Open(path.Join(l.basePath, filePath)); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(file)

	var line string
	for {
		if line, err = reader.ReadString('\n'); err != nil {
			break
		}
		if line[0] == '#' {
			// comment line
			continue
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
func combine(msgs []batter.Msg) []batter.Msg {
	if len(msgs) == 0 {
		return msgs
	}
	maxMsg := msgs[0].(*MaxValueMsg)
	for _, msg := range msgs {
		if msg.(*MaxValueMsg).Value > maxMsg.Value {
			maxMsg = msg.(*MaxValueMsg)
		}
	}
	return []batter.Msg{maxMsg}
}

// Do work
func (v *MaxValueVertex) Compute(msgs []waffle.Msg) {
	start := time.Now()
	if val := v.AggregateValue("timing"); v.Superstep() > 0 && val != nil {
		if dur, ok := val.(int64); ok && dur > 5*1e8 {
			log.Printf("timing: %d nanoseconds", val)
		}
	}
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
	v.SubmitToAggregator("timing", int(time.Now().Sub(start)))
}

type TimingAggregator struct {
	values []int
}

func (a *TimingAggregator) Name() string {
	return "timing"
}

func (a *TimingAggregator) Reset() {
	a.values = a.values[0:0]
}

func (a *TimingAggregator) Submit(v interface{}) {
	if dur, ok := v.(int); ok {
		a.values = append(a.values, dur)
	} else {
		panic("non int value submitted to TimingAggregator")
	}
}

func (a *TimingAggregator) ReduceAndEmit() interface{} {
	if len(a.values) == 0 {
		return 0
	}
	var sum int = 0
	for _, dur := range a.values {
		sum += dur
	}
	return sum / len(a.values)
}

var master bool
var host, port, maddr, loadDir, persistDir string
var minWorkers int

func main() {

	flag.BoolVar(&master, "master", false, "node is master")
	flag.StringVar(&maddr, "maddr", "127.0.0.1:50000", "master address")
	flag.StringVar(&port, "port", "50000", "node port")
	flag.StringVar(&host, "host", "127.0.0.1", "node address")
	flag.IntVar(&minWorkers, "minWorkers", 2, "min workers")
	flag.StringVar(&loadDir, "loadDir", "testdata", "data load path")
	flag.StringVar(&persistDir, "persistDir", "persist", "data persist path")

	flag.Parse()

	gob.Register(&MaxValueVertex{})
	gob.Register(&MaxValueMsg{})

	persister := waffle.NewGobPersister(persistDir)
	loader := &MaxValueLoader{basePath: loadDir}

	if master {
		m := waffle.NewMaster(host, port, new(batter.GobRPCMasterServer), new(batter.GobRPCMasterWorkerClient))

		m.Config.JobId = "maxval-" + time.Now().UTC().String()
		m.Config.MinWorkers = minWorkers
		m.Config.HeartbeatInterval = 10 * 1e9

		m.SetPersister(persister)
		m.SetLoader(loader)
		m.SetCheckpointFn(func(checkpoint uint64) bool {
			return true
		})
		m.AddAggregator(&TimingAggregator{})
		m.Config.LoadPaths = []string{loadDir}
		m.Start()
	} else {
		w := waffle.NewWorker(host, port, new(batter.GobRPCWorkerServer), new(batter.GobRPCWorkerMasterClient), new(batter.GobRPCWorkerWorkerClient))
		w.Config.MessageThreshold = 1000
		w.Config.VertexThreshold = 10

		w.Config.MasterHost, w.Config.MasterPort, _ = net.SplitHostPort(maddr)

		w.SetLoader(loader)
		w.SetPersister(persister)
		w.SetResultWriter(&MaxValueResultWriter{})
		w.AddCombiner(combine)
		w.AddAggregator(&TimingAggregator{})
		w.Start()
	}
}
