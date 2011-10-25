package waffle

import (
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type phaseStat struct {
	startTime int64
	endTime   int64
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

func (s *phaseStat) addError(err os.Error) {
	// pass
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

type Worker struct {
	node
	workerId string
	jobId    string

	mhost string
	mport string

	state int

	lastCheckpoint bool
	lastSuperstep  uint64

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
}

// Worker state
const (
	NONE = iota
	INIT
	WAIT
	REGISTER
	LOAD_1
	LOAD_2
	COMPUTE
	STORE
)

func NewWorker(addr, port string, msgThreshold, vertThreshold int64) *Worker {
	w := &Worker{
		state:      NONE,
		partitions: make(map[uint64]*Partition),
	}

	w.msgs = newInMsgQ()
	w.inq = newInMsgQ()
	w.outq = newOutMsgQ(w, msgThreshold)
	w.vinq = newInVertexQ()
	w.voutq = newOutVertexQ(w, vertThreshold)

	w.stepInfo, w.lastStepInfo = &stepInfo{}, &stepInfo{}

	w.InitNode(addr, port)

	return w
}

func (w *Worker) WorkerId() string {
	return w.workerId
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

// XXX temp function until theres some sort of discovery mechanism
func (w *Worker) SetMasterAddress(host, port string) {
	w.mhost, w.mport = host, port
}

func (w *Worker) Partitions() map[uint64]*Partition {
	return w.partitions
}

// Expose for RPC interface
func (w *Worker) SetTopology(workerMap map[string]string, partitionMap map[uint64]string) {
	w.workerMap = workerMap
	w.partitionMap = partitionMap

	for pid, wid := range w.partitionMap {
		if wid == w.workerId {
			w.partitions[pid] = NewPartition(pid, w)
		}
	}
}

// Expose for RPC interface
func (w *Worker) ExecPhase(exec *PhaseExec) os.Error {
	// Reset phase stats
	w.phaseStats.reset()
	w.phaseStats.start()

	// Determine the phaseId and dispatch
	switch exec.PhaseId {
	case phaseLOAD1:
		go w.loadPhase1()
	case phaseLOAD2:
		go w.loadPhase2()
	case phaseSTEPPREPARE:
		go w.prepareForStep()
	case phaseSUPERSTEP:
		go w.step(exec)
	case phaseWRITE:
		go w.outputResults()
	default:
		panic(os.NewError("No phase identified"))
	}
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

func (w *Worker) addVertex(v Vertex) {
	// determine the partition for v.  if it is not on this worker, add v to voutq so
	// we can send it to the correct worker
	pid := w.getPartitionOf(v.VertexId())
	wid := w.partitionMap[pid]
	if wid == w.workerId {
		w.partitions[pid].addVertex(v)
	} else {
		w.voutq.addVertex(v)
	}
}

func (w *Worker) sendSummary(phaseId int) {
	w.phaseStats.end()

	ps := &PhaseSummary{PhaseId: phaseId}

	ps.JobId = w.jobId
	ps.WorkerId = w.workerId
	ps.PhaseTime = w.phaseStats.endTime - w.phaseStats.startTime
	ps.ActiveVerts = 0
	ps.NumVerts = 0
	for _, p := range w.partitions {
		ps.ActiveVerts += p.numActiveVertices()
		ps.NumVerts += p.numVertices()
	}
	ps.SentMsgs = w.outq.numSent()

	w.rpcClient.SendSummary(net.JoinHostPort(w.mhost, w.mport), ps)
}

func (w *Worker) discoverMaster() os.Error {
	// TODO Discover master
	return nil
}

// Register step
func (w *Worker) register() (err os.Error) {
	log.Println("Trying to register")
	if w.workerId, w.jobId, err = w.rpcClient.Register(net.JoinHostPort(w.mhost, w.mport), w.Host(), w.Port()); err != nil {
		return
	}
	log.Printf("Registered as %s for job %s", w.workerId, w.jobId)
	return
}

func (w *Worker) cleanup() os.Error {
	// w.rpcClient.Cleanup()
	// w.rpcServ.Cleanup()
	return nil
}

func (w *Worker) loadPhase1() {
	if w.loader != nil {
		// At some point, should add code to load from the queue while
		// direct loading is going on.
		w.loader.Init(w)
		if loaded, err := w.loader.Load(); err == nil {
			log.Printf("Loaded %d vertices", loaded)
			w.voutq.flush()
			w.voutq.wait.Wait()
		} else {
			w.phaseStats.addError(err)
		}
	} else {
		log.Printf("worker %d has no loader", w.workerId)
	}

	go w.sendSummary(phaseLOAD1)
}

func (w *Worker) loadPhase2() {
	for _, v := range w.vinq.verts {
		w.addVertex(v)
	}

	go w.sendSummary(phaseLOAD2)
}

// Prepase for the next superstep (message queue swaps and resets)
func (w *Worker) prepareForStep() {
	w.msgs, w.inq = w.inq, w.msgs
	w.inq.clear()
	w.outq.reset()
	w.lastStepInfo, w.stepInfo = w.stepInfo, w.lastStepInfo

	log.Println("StepPrepare complete")

	go w.sendSummary(phaseSTEPPREPARE)
}

// Execute a single superstep
func (w *Worker) step(pe *PhaseExec) {

	superstep, checkpoint := pe.Superstep, pe.Checkpoint
	if superstep > 0 && w.lastStepInfo.superstep+1 != superstep {
		w.phaseStats.addError(os.NewError("Superstep did not increment by one"))
		go w.sendSummary(phaseSUPERSTEP)
		return
	}

	if checkpoint {
		if w.persister != nil {
			if err := w.persister.Write(w); err != nil {
				w.phaseStats.addError(err)
				go w.sendSummary(phaseSUPERSTEP)
				return
			}
		} else {
			log.Println("No Persister defined for this worker")
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

	go w.sendSummary(phaseSUPERSTEP)
}

func (w *Worker) outputResults() {
	if w.resultWriter != nil {
		if err := w.resultWriter.WriteResults(w); err != nil {
			w.phaseStats.addError(err)
		}
		log.Println("WriteResults complete")
	} else {
		log.Println("No ResultWriter set for this worker")
	}

	go w.sendSummary(phaseWRITE)
}

func (w *Worker) Run() {
	w.rpcClient.Init()
	w.rpcServ.Start(w)

	done := make(chan int)
	for {
		if err := w.discoverMaster(); err != nil {
			panic(err)
		}
		if err := w.register(); err != nil {
			panic(err)
		}
		if w.jobId != "" {
			break
		}
		log.Printf("Job registration unsuccessful.  Trying again.")
	}
	<-done
}
