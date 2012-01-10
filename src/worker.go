package waffle

import (
	"batter"
	// "errors"
	"log"
	"net"
	// "sync"
	"time"
)

type phaseStat struct {
	startTime int
	endTime   int
	sentMsgs  uint64
}

func (s *phaseStat) reset() {
	s.startTime = 0
	s.endTime = 0
}

func (s *phaseStat) start() {
	s.startTime = time.Now().Nanosecond()
}

func (s *phaseStat) end() {
	s.endTime = time.Now().Nanosecond()
}

type stepInfo struct {
	activeVertices uint64
	numVertices    uint64
	sentMsgs       uint64
	superstep      uint64
	checkpoint     bool
	aggregates     map[string]interface{}
}

type jobStat struct {
	numVertices uint64
}

type WorkerConfig struct {
	MessageThreshold int
	VertexThreshold  int
	Host             string
	Port             string
	MasterHost       string
	MasterPort       string
	RegisterRetry    uint64
}

type Worker struct {
	batter.Worker

	node
	jobId string

	state int

	lastCheckpoint bool
	lastSuperstep  uint64

	Config WorkerConfig

	phaseStats phaseStat
	jobStats   jobStat

	// Msg queues
	// msgs  *InMsgQ
	minq        *batter.InQ
	moutq       *batter.OutQ
	msgs, nmsgs map[string][]Msg

	vinq    *batter.InQ
	voutq   *batter.OutQ
	vertBuf map[string][]Vertex

	partitions      map[uint64]*Partition
	loadAssignments map[string][]string

	loader       Loader
	resultWriter ResultWriter
	persister    Persister
	combiners    []Combiner

	stepInfo, lastStepInfo *stepInfo

	mClient batter.WorkerMasterClient
	wClient batter.WorkerWorkerClient

	endCh chan *PhaseSummary

	done chan int
}

func NewWorker(addr, port string) *Worker {
	w := &Worker{
		partitions: make(map[uint64]*Partition),
	}

	w.stepInfo, w.lastStepInfo = &stepInfo{}, &stepInfo{}

	w.initNode(addr, port)

	return w
}

// Most of this stuff is exposed for persisters

func (w *Worker) WorkerId() string {
	return net.JoinHostPort(w.host, w.port)
}

func (w *Worker) Superstep() uint64 {
	// XXX ehhh, maybe its best to get rid of the alternating step infos...
	if w.stepInfo == nil {
		return 0
	}
	return w.stepInfo.superstep
}

func (w *Worker) Partitions() map[uint64]*Partition {
	return w.partitions
}

/*
func (w *Worker) IncomingMsgs() map[string][]Msg {
	return w.inq.in
}

func (w *Worker) SetRpcClient(c WorkerRpcClient) {
	w.rpcClient = c
}

func (w *Worker) SetRpcServer(s WorkerRpcServer) {
	w.rpcServ = s
}
*/

// The loader handles loading vertices and edges from the initial data source
func (w *Worker) SetLoader(l Loader) {
	w.loader = l
}

// The result writer writes the final results of the job
func (w *Worker) SetResultWriter(rw ResultWriter) {
	w.resultWriter = rw
}

// The persister reads and write data that is persisted at checkpoints
func (w *Worker) SetPersister(p Persister) {
	w.persister = p
}

// Add a message combiner
func (w *Worker) AddCombiner(c Combiner) {
	if w.combiners == nil {
		w.combiners = make([]Combiner, 0)
	}
	w.combiners = append(w.combiners, c)
}

// Expose for RPC interface
func (w *Worker) SetTopology(ti *TopologyInfo) {
	w.partitionMap = ti.PartitionMap
	log.Printf("partitions and load assignments set")

	for pid, hp := range w.partitionMap {
		if hp == w.WorkerId() {
			w.partitions[pid] = NewPartition(pid, w)
			log.Printf("created partition %d on %s", pid, w.WorkerId())
		}
	}
}

/*
// Expose for RPC interface
func (w *Worker) QueueMessages(msgs []Msg) {
	go w.inq.addMsgs(msgs)
}

// Expose for RPC interface
func (w *Worker) QueueVertices(verts []Vertex) {
	go w.vinq.addVertices(verts)
}
*/

func (w *Worker) AddVertex(v Vertex) {
	// determine the partition for v.  if it is not on this worker, add v to voutq so
	// we can send it to the correct worker
	pid := w.partitionOf(v.VertexId())
	wid := w.partitionMap[pid]
	if wid == w.WorkerId() {
		w.partitions[pid].addVertex(v)
	} else {
		w.SendVertex(wid, v)
	}
}

func (w *Worker) SendVertex(workerId string, v Vertex) {
	w.vertBuf[workerId] = append(w.vertBuf[workerId], v)
	if len(w.vertBuf[workerId]) >= w.Config.VertexThreshold {
		vmsg := &VertexMsg{Vertices: w.vertBuf[workerId]}
		vmsg.SetTarget(workerId)
		w.voutq.Funnel <- vmsg
		delete(w.vertBuf, workerId)
	}
}

func (w *Worker) FlushVertices() {
	for workerId, vertices := range w.vertBuf {
		vmsg := &VertexMsg{Vertices: vertices}
		vmsg.SetTarget(workerId)
		w.voutq.Funnel <- vmsg
		delete(w.vertBuf, workerId)
	}
}

/*
func loadData(w *Worker, e PhaseExec) PhaseSummary {
	pe := e.(*LoadDataExec)

	ps := &LoadDataSummary{}
	ps.WId = w.WorkerId()

	var thisWorker []string
	var ok bool
	if thisWorker, ok = pe.LoadAssignments[w.WorkerId()]; !ok {
		log.Printf("no load assignments for worker %s", w.WorkerId())
		return ps
	}

	if w.loader == nil {
		ps.Error = "No loader to load assignment on worker " + w.WorkerId()
		return ps
	}

	var totalLoaded uint64 = 0
	for _, path := range thisWorker {
		if loaded, err := w.loader.Load(w, path); err != nil {
			log.Printf("Error loading data: %v", err)
			ps.Error = err.Error()
			return ps
		} else {
			totalLoaded += loaded
		}
	}

	log.Printf("loaded %d vertices", totalLoaded)
	w.voutq.flush()
	w.voutq.wait.Wait()
	return ps
}

func distributeVertices(w *Worker, e PhaseExec) PhaseSummary {
	// pe := e.(*LoadRecievedExec)
	ps := &LoadRecievedSummary{}
	ps.WId = w.WorkerId()

	for _, v := range w.vinq.verts {
		w.AddVertex(v)
	}

	for _, p := range w.partitions {
		ps.TotalVerts += p.numVertices()
		ps.ActiveVerts += p.numActiveVertices()
	}

	return ps
}
*/
// load from persistence
func loadPersisted(w *Worker, pe PhaseExec) PhaseSummary {
	panic("not implemented")
	/*
		superstep := pe.Superstep
		if w.persister != nil {
			for _, part := range w.partitions {
				vertices, inbound, err := w.persister.LoadPartition(part.id, superstep)
				if err != nil {
					return err
				}
				for _, vertex := range vertices {
					part.addVertex(vertex)
				}
				w.inq.addMsgs(inbound)
			}
		} else {
			log.Printf("worker %s has no persister", w.WorkerId())
		}
	*/
	return nil
}

// Set the recovered superstep
func recover(w *Worker, pe PhaseExec) PhaseSummary {
	panic("not implemented")
	/*
		w.stepInfo.superstep = pe.Superstep
		// to get through the increment check in step()
		if w.stepInfo.superstep > 0 {
			w.lastStepInfo.superstep -= 1
		}
	*/
	return nil
}

/*
// Prepase for the next superstep (message queue swaps and resets)
func stepPrepare(w *Worker, e PhaseExec) PhaseSummary {
	// pe := e.(*StepPrepareExec)
	ps := &StepPrepareSummary{}
	ps.WId = w.WorkerId()

	w.msgs, w.inq = w.inq, w.msgs
	w.inq.clear()
	w.outq.reset()
	w.lastStepInfo, w.stepInfo = w.stepInfo, w.lastStepInfo
	return ps
}
*/

/*
func (w *Worker) persistPartitions() (err error) {
	if w.persister == nil {
		log.Printf("worker %s has no persister", w.WorkerId())
		return
	}

	verts, msgs := make([]Vertex, 0), make([]Msg, 0)
	for pid, part := range w.partitions {
		verts = verts[0:0]
		msgs = msgs[0:0]
		for _, vlist := range part.verts {
			verts = append(verts, vlist)
		}
		for _, v := range verts {
			if vmsgs := w.msgs[v.VertexId()]; vmsgs != nil {
				msgs = append(msgs, vmsgs...)
			}
		}
		if err = w.persister.PersistPartition(pid, w.stepInfo.superstep, verts, msgs); err != nil {
			return
		}
		log.Printf("partition %d, superstep %d: persisted %d vertices, %d messages", pid, w.stepInfo.superstep, len(verts), len(msgs))
	}
	return
}

// Execute a single superstep
func step(w *Worker, e PhaseExec) PhaseSummary {
	pe := e.(*SuperstepExec)

	ps := &SuperstepSummary{
		Aggregates: make(map[string]interface{}),
	}
	ps.WId = w.WorkerId()

	superstep, checkpoint := pe.Superstep, pe.Checkpoint
	if superstep > 0 && w.lastStepInfo.superstep+1 != superstep {
		ps.Error = "Superstep did not increment by one"
		return ps
	}

	// set the step info fields for superstep and checkpoint
	w.stepInfo.superstep, w.stepInfo.checkpoint = superstep, checkpoint

	if w.stepInfo.checkpoint {
		if err := w.persistPartitions(); err != nil {
			ps.Error = err.Error()
			return ps
		}
	}

	// reset aggregators
	for _, aggr := range w.aggregators {
		aggr.Reset()
	}

	w.stepInfo.aggregates = pe.Aggregates

	var wg sync.WaitGroup
	// XXX limit max routines?
	// XXX use real threads?
	for _, p := range w.partitions {
		pp := p
		wg.Add(1)
		go func() {
			pp.compute()
			wg.Done()
		}()
	}
	wg.Wait()

	// Flush the outq and wait for any messages that haven't been sent yet
	w.outq.flush()
	w.outq.wait.Wait()

	for key, aggr := range w.aggregators {
		ps.Aggregates[key] = aggr.ReduceAndEmit()
	}

	ps.SentMsgs = w.outq.numSent()
	for _, p := range w.partitions {
		ps.ActiveVerts += p.numActiveVertices()
	}

	log.Printf("Superstep %d complete", w.stepInfo.superstep)
	log.Printf("sent %d messages and have %d active verts", ps.SentMsgs, ps.ActiveVerts)
	return ps
}

func writeResults(w *Worker, pe PhaseExec) PhaseSummary {
	// XXX temp kill until i add a shutdown phase
	defer func() { w.done <- 1 }() // XXX sometimes this kills before the summary for the phase is sent
	ps := &WriteResultsSummary{}
	ps.WId = w.WorkerId()
	if w.resultWriter != nil {
		if err := w.resultWriter.WriteResults(w); err != nil {
			ps.Error = err.Error()
		}
	} else {
		log.Println("worker %s has no resultWriter", w.WorkerId())
	}
	return ps
}

func (w *Worker) shutdown() {
	log.Printf("worker %s shutting down", w.WorkerId())
	return
}
*/

func infoMerge(existing *stepInfo, new *stepInfo) *stepInfo {
	return nil
}

func (w *Worker) Start() {
	w.Init(w.Config.Host, w.Config.Port, w.mClient, w.wClient)

	w.voutq, _ = w.CreateMsgOutQueue("v", w.Config.VertexThreshold)
	w.vinq, _ = w.CreateMsgInQueue("v")

	w.moutq, _ = w.CreateMsgOutQueue("m", w.Config.MessageThreshold)
	w.minq, _ = w.CreateMsgInQueue("m")

	w.OnTaskReceive(func(t batter.Task) {
		if t, ok := t.(WaffleTask); ok {
			t.SetWorker(w)
		}
	})

	w.Run(w.Config.MasterHost, w.Config.MasterPort)

	<-make(chan byte)

	/*
		w.done = make(chan int)
		for i := 0; uint64(i) < w.Config.RegisterRetry+1; i++ {
			// TODO: There should be a better check here.  Use an error for an unsuccessful registration, 
			// but err == nil && jobId == "" should be an error.
			if err := w.register(); err != nil {
				panic(err)
			}
			if w.jobId != "" {
				break
			}
			log.Printf("Job registration unsuccessful.  Trying again.")
		}
		if w.jobId == "" {
			log.Println("Failed to register for job, shutting down.")
			w.done <- 1
		}
		<-w.done
		w.shutdown()
	*/
}
