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

type Component struct {
	w *Worker
}

func (c *Component) Init(w *Worker) {
	c.w = w
}

func (c *Component) Worker() *Worker {
	return c.w
}

type Worker struct {
	node
	workerId string
	jobId    string

	mhost string
	mport string

	state int
	// Current and previous state
	// checkpoint bool
	// superstep  uint64

	lastCheckpoint bool
	lastSuperstep  uint64

	numVertices uint64
	phaseStats  phaseStat
	jobStats    jobStat

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

	// phases    map[int]Phase
	rpcClient WorkerRpcClient
	rpcServ   WorkerRpcServer

	evCh chan *PhaseSummary
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
	w.InitNode(addr, port)
	w.msgs = newInMsgQ()
	w.inq = newInMsgQ()
	w.outq = newOutMsgQ(w, msgThreshold)
	w.vinq = newInVertexQ()
	w.voutq = newOutVertexQ(w, vertThreshold)

	w.stepInfo, w.lastStepInfo = &stepInfo{}, &stepInfo{}

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
	w.loader.Init(w)
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

func (w *Worker) NumActiveVertices() uint64 {
	var sum uint64 = 0
	for _, p := range w.partitions {
		sum += p.numActiveVertices()
	}
	return sum
}

func (w *Worker) NumVertices() uint64 {
	var sum uint64 = 0
	for _, p := range w.partitions {
		sum += p.numVertices()
	}
	return sum
}

func (w *Worker) NumSentMsgs() uint64 {
	return w.outq.numSent()
}

// Expose for RPC interface
func (w *Worker) ExecPhase(exec *PhaseExec) os.Error {
	// Reset phase stats
	w.phaseStats.reset()
	w.phaseStats.start()

	// Determine the phaseId and dispatch
	switch exec.PhaseId {
	case phaseLOAD1:
		go w.executeLoadDirect()
	case phaseLOAD2:
		go w.executeLoadQueue()
	case phaseSTEPPREPARE:
		go w.executeStepPrepare()
	case phaseSUPERSTEP:
		go w.executeSuperstep(exec.Superstep, exec.Checkpoint)
	case phaseWRITE:
		go w.executeWriteResults()
	default:
		panic(os.NewError("No phase identified"))
	}
	return nil
}

func (w *Worker) Run() {
	w.rpcClient.Init()
	w.rpcServ.Start(w)

	// w.init()
	if err := w.discoverMaster(); err != nil {
		panic(err)
	}

	if err := w.registerForJob(); err != nil {
		panic(err)
	}
	// XXX Just die if registration didn't go through
	if w.jobId == "" {
		log.Printf("No job, bye bye")
		return
	}
	w.phaseSummaryLoop()
}

func (w *Worker) phaseSummaryLoop() {
	w.evCh = make(chan *PhaseSummary)
	for ev := range w.evCh {
		// end the phase and fill in some of the summary
		w.phaseStats.end()

		ev.PhaseTime = w.phaseStats.endTime - w.phaseStats.startTime
		ev.WorkerId = w.workerId
		w.rpcClient.PhaseResult(net.JoinHostPort(w.mhost, w.mport), ev)
	}
}

func (w *Worker) discoverMaster() os.Error {
	// TODO Discover master
	return nil
}

// Register step
func (w *Worker) registerForJob() (err os.Error) {
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

// Expose for RPC interface
func (w *Worker) SetJobTopology(workerMap map[string]string, partitionMap map[uint64]string) {
	w.workerMap = workerMap
	w.partitionMap = partitionMap

	for pid, wid := range w.partitionMap {
		if wid == w.workerId {
			w.partitions[pid] = NewPartition(pid, w)
		}
	}
}

func (w *Worker) executeLoadDirect() {
	var summary PhaseSummary

	if w.loader == nil {
		summary.addError(os.NewError("Worker has no loader"))
		w.evCh <- &summary
		return
	}

	loaded, err := w.loader.Load()
	if err != nil {
		summary.addError(err)
		w.evCh <- &summary
		return
	}

	w.voutq.flush()
	w.voutq.wait.Wait()

	summary.NumVerts = loaded
	summary.PhaseId = phaseLOAD1

	log.Println("LoadDirect complete")

	w.evCh <- &summary
}

func (w *Worker) executeLoadQueue() {
	var loaded uint64 = 0
	for _, v := range w.vinq.verts {
		w.addToPartition(v)
		loaded++
	}

	var summary PhaseSummary

	summary.PhaseId = phaseLOAD2
	summary.NumVerts = loaded
	summary.ActiveVerts = w.NumActiveVertices()

	log.Println("LoadQueue complete")

	w.evCh <- &summary
}

func (w *Worker) addToPartition(v Vertex) os.Error {
	// determine the partition for v.  if it is not on this worker, add v to voutq so
	// we can send it to the correct worker
	pid := w.getPartitionOf(v.VertexId())
	wid := w.partitionMap[pid]
	if wid == w.workerId {
		w.partitions[pid].addVertex(v)
	} else {
		w.voutq.addVertex(v)
	}
	return nil
}

// Expose for RPC iterface
func (w *Worker) executeStepPrepare() {
	w.msgs, w.inq = w.inq, w.msgs
	w.inq.clear()
	w.outq.reset()
	w.lastStepInfo, w.stepInfo = w.stepInfo, w.lastStepInfo

	log.Println("StepPrepare complete")

	w.evCh <- &PhaseSummary{PhaseId: phaseSTEPPREPARE}
}

func (w *Worker) compute() os.Error {
	var wg sync.WaitGroup
	// XXX limit max routines?
	for _, p := range w.partitions {
		pp := p
		wg.Add(1)
		go func() {
			pp.compute()
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func (w *Worker) executeSuperstep(superstep uint64, checkpoint bool) {
	summary := &PhaseSummary{PhaseId: phaseSUPERSTEP}

	if superstep > 0 && w.lastStepInfo.superstep+1 != superstep {
		summary.addError(os.NewError("Superstep did not increment by one"))
		w.evCh <- summary
		return
	}

	if checkpoint {
		if w.persister != nil {
			if err := w.persister.Write(); err != nil {
				summary.addError(err)
				w.evCh <- summary
				return
			}
		} else {
			log.Println("No Persister defined for this worker")
		}
	}

	// set the step info fields for superstep and checkpoint
	w.stepInfo.superstep, w.stepInfo.checkpoint = superstep, checkpoint

	w.compute()

	// Flush the outq and wait for any messages that haven't been sent yet
	w.outq.flush()
	w.outq.wait.Wait()

	summary.JobId = w.jobId
	summary.ActiveVerts = w.NumActiveVertices()
	summary.NumVerts = w.NumVertices()
	summary.SentMsgs = w.NumSentMsgs()

	log.Println("Superstep complete")

	w.evCh <- summary
}

// Expose for RPC interface
func (w *Worker) QueueMessages(msgs []Msg) {
	go w.inq.addMsgs(msgs)
}

// Expose for RPC interface
func (w *Worker) QueueVertices(verts []Vertex) {
	go w.vinq.addVertices(verts)
}

func (w *Worker) executeWriteResults() {
	summary := &PhaseSummary{PhaseId: phaseWRITE}
	if w.resultWriter == nil {
		log.Println("No ResultWriter defined for this worker")
		w.evCh <- summary
		return
	}

	w.resultWriter.Init(w)
	if err := w.resultWriter.WriteResults(); err != nil {
		summary.addError(err)
	}

	log.Println("WriteResults complete")

	w.evCh <- summary
}
