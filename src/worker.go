package waffle

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type phaseFn func(*Worker, *PhaseExec) error

var phaseMap map[int]phaseFn = map[int]phaseFn{
	phaseLOAD1:       loadPhase1,
	phaseLOAD2:       loadPhase2,
	phaseLOAD3:       loadPhase3,
	phaseRECOVER:     recover,
	phaseSTEPPREPARE: stepPrepare,
	phaseSUPERSTEP:   step,
	phaseWRITE:       writeResults,
}

type phaseStat struct {
	startTime int64
	endTime   int64
	error     error
}

func (s *phaseStat) reset() {
	s.startTime = 0
	s.endTime = 0
}

func (s *phaseStat) start() {
	s.startTime = time.Seconds()
}

func (s *phaseStat) end() {
	s.endTime = time.Seconds()
}

type stepInfo struct {
	activeVertices uint64
	numVertices    uint64
	sentMsgs       uint64
	superstep      uint64
	checkpoint     bool
}

type jobStat struct {
	numVertices uint64
}

type WorkerConfig struct {
	MessageThreshold int64
	VertexThreshold  int64
	MasterHost       string
	MasterPort       string
	RegisterRetry    uint64
}

type Worker struct {
	node
	workerId string
	jobId    string

	state int

	lastCheckpoint bool
	lastSuperstep  uint64

	Config WorkerConfig

	phaseStats phaseStat
	jobStats   jobStat

	// Msg queues
	msgs  *InMsgQ
	inq   *InMsgQ
	outq  *OutMsgQ
	vinq  *InVertexQ
	voutq *OutVertexQ

	partitions map[uint64]*Partition

	loader       Loader
	resultWriter ResultWriter
	persister    Persister
	aggregators  []Aggregator
	combiners    []Combiner

	stepInfo, lastStepInfo *stepInfo

	rpcClient WorkerRpcClient
	rpcServ   WorkerRpcServer

	endCh chan *PhaseSummary

	done chan int
}

func NewWorker(addr, port string) *Worker {
	w := &Worker{
		partitions: make(map[uint64]*Partition),
	}

	w.stepInfo, w.lastStepInfo = &stepInfo{}, &stepInfo{}

	w.InitNode(addr, port)

	return w
}

// Most of this stuff is exposed for persisters

func (w *Worker) WorkerId() string {
	return w.workerId
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

func (w *Worker) IncomingMsgs() map[string][]Msg {
	return w.inq.in
}

func (w *Worker) SetRpcClient(c WorkerRpcClient) {
	w.rpcClient = c
}

func (w *Worker) SetRpcServer(s WorkerRpcServer) {
	w.rpcServ = s
}

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
func (w *Worker) SetTopology(workerMap map[string]string, partitionMap map[uint64]string) {
	w.workerMap = workerMap
	w.partitionMap = partitionMap
	log.Printf("topology set")

	for pid, wid := range w.partitionMap {
		if wid == w.workerId {
			w.partitions[pid] = NewPartition(pid, w)
			log.Printf("create partitions %d on %s", pid, wid)
		}
	}
}

// execute a phase function
func (w *Worker) executePhase(phaseFn phaseFn, exec *PhaseExec) {
	w.phaseStats.reset()
	w.phaseStats.start()
	if err := phaseFn(w, exec); err != nil {
		log.Printf("phaseFn finished with error: %v", err)
		w.phaseStats.error = err
	}
	w.phaseStats.end()
	if err := w.sendSummary(exec.PhaseId); err != nil {
		// handle summary send failure
	}
}

// Expose for RPC interface
// ExecPhase only returns a non nil error if the phase cannot be identified
func (w *Worker) ExecPhase(exec *PhaseExec) error {
	// Determine the phaseId and dispatch
	var fn phaseFn
	var ok bool
	if fn, ok = phaseMap[exec.PhaseId]; !ok {
		return errors.New("No valid phase specified")
	}
	go w.executePhase(fn, exec)
	return nil
}

// Expose for RPC interface
func (w *Worker) QueueMessages(msgs []Msg) {
	go w.inq.addMsgs(msgs)
}

// Expose for RPC interface
func (w *Worker) QueueVertices(verts []Vertex) {
	go w.vinq.addVertices(verts)
}

func (w *Worker) AddVertex(v Vertex) {
	// determine the partition for v.  if it is not on this worker, add v to voutq so
	// we can send it to the correct worker
	pid := w.partitionOf(v.VertexId())
	wid := w.partitionMap[pid]
	if wid == w.workerId {
		w.partitions[pid].addVertex(v)
	} else {
		w.voutq.addVertex(v)
	}
}

func (w *Worker) sendSummary(phaseId int) error {
	ps := &PhaseSummary{
		PhaseId:     phaseId,
		JobId:       w.jobId,
		WorkerId:    w.workerId,
		PhaseTime:   w.phaseStats.endTime - w.phaseStats.startTime,
		SentMsgs:    w.outq.numSent(),
		ActiveVerts: 0,
		NumVerts:    0,
		Error:       w.phaseStats.error,
	}

	for _, p := range w.partitions {
		ps.ActiveVerts += p.numActiveVertices()
		ps.NumVerts += p.numVertices()
	}

	return w.rpcClient.SendSummary(net.JoinHostPort(w.Config.MasterHost, w.Config.MasterPort), ps)
}

// Register step
func (w *Worker) register() (err error) {
	log.Println("Trying to register")
	if w.workerId, w.jobId, err = w.rpcClient.Register(net.JoinHostPort(w.Config.MasterHost, w.Config.MasterPort), w.Host(), w.Port()); err != nil {
		return
	}
	log.Printf("Registered as %s for job %s", w.workerId, w.jobId)
	return
}

func (w *Worker) cleanup() error {
	// w.rpcClient.Cleanup()
	// w.rpcServ.Cleanup()
	return nil
}

func loadPhase1(w *Worker, pe *PhaseExec) error {
	if w.loader != nil {
		if loaded, err := w.loader.Load(w); err != nil {
			return err
		} else {
			log.Printf("loaded %d vertices", loaded)
			w.voutq.flush()
			w.voutq.wait.Wait()
		}
	} else {
		log.Printf("worker %s has no loader", w.workerId)
	}
	return nil
}

func loadPhase2(w *Worker, pe *PhaseExec) error {
	for _, v := range w.vinq.verts {
		w.AddVertex(v)
	}
	return nil
}

// load from persistence
func loadPhase3(w *Worker, pe *PhaseExec) error {
	superstep := pe.Superstep
	if w.persister != nil {
		for _, part := range w.partitions {
			vertices, inbound, err := w.persister.Load(part.id, superstep)
			if err != nil {
				return err
			}
			for _, vertex := range vertices {
				part.addVertex(vertex)
			}
			w.inq.addMsgs(inbound)
		}
	} else {
		log.Printf("worker %s has no persister", w.workerId)
	}
	return nil
}

// Set the recovered superstep
func recover(w *Worker, pe *PhaseExec) error {
	w.stepInfo.superstep = pe.Superstep
	// to get through the increment check in step()
	if w.stepInfo.superstep > 0 {
		w.lastStepInfo.superstep -= 1
	}
	return nil
}

// Prepase for the next superstep (message queue swaps and resets)
func stepPrepare(w *Worker, pe *PhaseExec) error {
	w.msgs, w.inq = w.inq, w.msgs
	w.inq.clear()
	w.outq.reset()
	w.lastStepInfo, w.stepInfo = w.stepInfo, w.lastStepInfo
	return nil
}

// Execute a single superstep
func step(w *Worker, pe *PhaseExec) error {
	superstep, checkpoint := pe.Superstep, pe.Checkpoint
	if superstep > 0 && w.lastStepInfo.superstep+1 != superstep {
		return errors.New("Superstep did not increment by one")
	}

	if checkpoint {
		if w.persister != nil {
			verts := make([]Vertex, 0)
			msgs := make([]Msg, 0)
			for pid, part := range w.partitions {
				verts = verts[0:0]
				msgs = msgs[0:0]
				for _, vlist := range part.verts {
					verts = append(verts, vlist)
				}
				for _, v := range verts {
					if vmsgs := w.msgs.msgs(v.VertexId()); vmsgs != nil {
						msgs = append(msgs, vmsgs...)
					}
				}
				if err := w.persister.Persist(pid, superstep, verts, msgs); err != nil {
					return err
				}
				log.Printf("Persister %d: %d vertices, %d messages", pid, len(verts), len(msgs))
			}
		} else {
			log.Printf("worker %s has no persister", w.workerId)
		}
	}

	// set the step info fields for superstep and checkpoint
	w.stepInfo.superstep, w.stepInfo.checkpoint = superstep, checkpoint

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

	log.Println("Superstep complete")
	return nil
}

func writeResults(w *Worker, pe *PhaseExec) error {
	// XXX temp kill until i add a shutdown phase
	defer func() { w.done <- 1 }()
	if w.resultWriter != nil {
		return w.resultWriter.WriteResults(w)
	} else {
		log.Println("worker %s has no resultWriter", w.workerId)
	}
	return nil
}

func (w *Worker) shutdown() {
	return
}

func (w *Worker) Run() {
	w.rpcClient.Init()
	w.rpcServ.Start(w)

	w.msgs = newInMsgQ()
	w.inq = newInMsgQ()
	w.outq = newOutMsgQ(w, w.Config.MessageThreshold)
	w.vinq = newInVertexQ()
	w.voutq = newOutVertexQ(w, w.Config.VertexThreshold)

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
}
