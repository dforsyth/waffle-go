package waffle

import (
	"http"
	"log"
	"net"
	"os"
	"rpc"
	"sync"
	"gob"
)

type workerStat struct {
	activeVertices uint64
	numVertices    uint64
	sentMsgs       uint64
}

type jobStat struct {
	numVertices uint64
}

type Worker struct {
	node
	wid   string
	JobId string

	maddr string
	mcl   *rpc.Client

	state int
	// Current and previous state
	checkpoint bool
	superstep  uint64

	lastCheckpoint bool
	lastSuperstep  uint64

	numVertices uint64
	workerStats workerStat
	jobStats    jobStat

	// Msg queues
	msgs  *InMsgQ
	inq   *InMsgQ
	outq  *OutMsgQ
	vinq  *InVertexQ
	voutq *OutVertexQ

	parts map[uint64]*Partition

	logger *log.Logger

	loader       Loader
	resultWriter ResultWriter
	persister    Persister
	aggregators  []Aggregator
	combiner     Combiner
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

func NewWorker(addr, port string) *Worker {
	w := &Worker{
		logger: log.New(os.Stdout, "(worker) "+net.JoinHostPort(addr, port)+": ", 0),
		state:  NONE,
		parts:  make(map[uint64]*Partition),
	}
	w.InitNode(addr, port)
	w.msgs = newInMsgQ()
	w.inq = newInMsgQ()
	w.outq = newOutMsgQ(w, 10)
	w.vinq = newInVertexQ()
	w.voutq = newOutVertexQ(w, 10)

	// Register the base types with gob
	gob.Register(&VertexBase{})
	gob.Register(&EdgeBase{})
	gob.Register(&MsgBase{})
	return w
}

// Type registration for the gob encoder/decoder
func (w *Worker) RegisterVertex(v Vertex) {
	gob.Register(v)
}

func (w *Worker) RegisterMessage(m Msg) {
	gob.Register(m)
}

func (w *Worker) RegisterEdge(e Edge) {
	gob.Register(e)
}

// Setters for loading and writing data

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

// XXX temp function until theres some sort of discovery mechanism
func (w *Worker) SetMasterAddress(addr string) {
	w.maddr = addr
}

func (w *Worker) InitWorker(addr, port string) {
	w.InitNode(addr, port)
}

func (w *Worker) MsgHandler(msgs []Msg, resp *Resp) os.Error {
	go w.inq.addMsgs(msgs)
	*resp = OK
	return nil
}

func (w *Worker) genWorkerInfoMsg(success bool) *WorkerInfoMsg {
	m := &WorkerInfoMsg{ActiveVerts: w.workerStats.activeVertices,
		NumVerts: w.workerStats.numVertices,
		SentMsgs: w.workerStats.sentMsgs,
		Success:  success}
	m.Wid = w.wid
	return m
}

func (w *Worker) Partitions() map[uint64]*Partition {
	return w.parts
}

func (w *Worker) NumActiveVertices() uint64 {
	var sum uint64 = 0
	for _, p := range w.parts {
		sum += p.numActiveVertices()
	}
	return sum
}

func (w *Worker) NumVertices() uint64 {
	var sum uint64 = 0
	for _, p := range w.parts {
		sum += p.numVertices()
	}
	return sum
}

func (w *Worker) NumSentMsgs() uint64 {
	return w.outq.numSent()
}

func (w *Worker) notifyMaster(call string) os.Error {
	var r Resp
	if err := w.mcl.Call(call, w.genWorkerInfoMsg(true), &r); err != nil {
		return err
	}
	return nil
}

func (w *Worker) Run() {
	done := make(chan byte)
	w.init()
	w.discoverMaster()
	w.register()
	// XXX Just die if registration didn't go through
	if w.JobId == "" {
		w.logger.Printf("No job, bye bye")
		return
	}
	w.state = WAIT
	<-done
}

func (w *Worker) init() os.Error {
	w.state = INIT
	rpc.Register(w)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", net.JoinHostPort(w.addr, w.port))
	if e != nil {
		return e
	}
	go http.Serve(l, nil)
	w.logger.Printf("init complete")
	return nil
}

func (w *Worker) discoverMaster() os.Error {
	// TODO Discover master
	return nil
}

func (w *Worker) register() os.Error {
	w.state = REGISTER
	mcl, err := rpc.DialHTTP("tcp", w.maddr)
	if err != nil {
		w.logger.Printf("Error dialing master:", err)
		return err
	}
	w.mcl = mcl

	var r RegisterResp
	if err = w.mcl.Call("Master.Register", &RegisterMsg{Addr: w.addr, Port: w.port}, &r); err != nil {
		w.logger.Printf("Error registering with master:", err)
		return err
	}
	if len(r.JobId) > 0 {
		w.wid = r.Wid
		w.JobId = r.JobId
		w.logger.Printf("Registered for %s as %s", w.JobId, w.wid)
	}
	w.logger.Printf("Done registering")
	return nil
}

func (w *Worker) Healthcheck(args *BasicMasterMsg, resp *Resp) os.Error {
	*resp = OK
	return nil
}

func (w *Worker) WorkerInfo(args *ClusterInfoMsg, resp *Resp) os.Error {
	w.logger.Printf("setting worker info")
	w.wmap = args.Wmap
	w.pmap = args.Pmap
	*resp = OK
	w.setupPartitions()
	return nil
}

// Create the partitions assigned to this worker
func (w *Worker) setupPartitions() {
	for pid, wid := range w.pmap {
		if wid == w.wid {
			w.parts[pid] = NewPartition(pid, w)
		}
	}
}

func (w *Worker) DataLoad(args *BasicMasterMsg, resp *Resp) os.Error {
	if args.JobId != w.JobId {
		*resp = NOT_OK
		return nil
	}
	*resp = OK
	go w.loadVertices()
	return nil
}

func (w *Worker) loadVertices() os.Error {
	w.logger.Printf("Worker %s is loading vertices from loader", w.wid)
	// load verts
	verts, e := w.loader.Load(w)
	if e != nil {
		panic(e.String())
	}
	w.logger.Printf("Worker %s loaded %d vertices", w.wid, len(verts))
	// distribute verts to partitions
	for _, v := range verts {
		w.addToPartition(v)
	}
	// flush the voutq before we report completion
	w.voutq.flush()
	w.collectWorkerInfo()
	if err := w.notifyMaster("Master.NotifyInitialDataLoadComplete"); err != nil {
		return err
	}
	return nil
}

func (w *Worker) SecondaryDataLoad(args *BasicMasterMsg, resp *Resp) os.Error {
	go w.loadMoreVertices()
	*resp = OK
	return nil
}

func (w *Worker) loadMoreVertices() os.Error {
	w.logger.Printf("Worker %s is loading vertices from vinq", w.wid)
	for _, v := range w.vinq.verts {
		w.logger.Printf("Loading %s", v.VertexId())
		w.addToPartition(v)
	}
	w.vinq.clear()
	w.collectWorkerInfo()
	if err := w.notifyMaster("Master.NotifySecondaryDataLoadComplete"); err != nil {
		return err
	}
	w.logger.Printf("done loading more vertices")
	return nil
}

func (w *Worker) addToPartition(v Vertex) os.Error {
	// determine the partition for v.  if it is not on this worker, add v to voutq so
	// we can send it to the correct worker
	pid := w.getPartitionOf(v.VertexId())
	wid := w.pmap[pid]
	w.logger.Printf("Adding %s to pid %d", v.VertexId(), pid)
	if wid == w.wid {
		w.parts[pid].addVertex(v)
	} else {
		w.logger.Printf("Adding %s to voutq", v.VertexId())
		w.voutq.addVertex(v)
	}
	return nil
}

func (w *Worker) QueueVertices(args []Vertex, resp *Resp) os.Error {
	*resp = OK
	w.vinq.addVertices(args)
	return nil
}

func (w *Worker) PrepareForSuperstep(args *BasicMasterMsg, resp *Resp) os.Error {
	*resp = OK
	go w.prepareForSuperstep()
	return nil
}

// This is step is just to let us swap message queues.  In the future I should
// probably just put a superstep value in Msgs so this is unnecessary?
func (w *Worker) prepareForSuperstep() os.Error {
	tmp := w.msgs
	w.msgs = w.inq
	tmp.clear()
	w.inq = tmp

	if err := w.notifyMaster("Master.NotifyPrepareComplete"); err != nil {
		return err
	}
	return nil
}

func (w *Worker) Superstep(args *SuperstepMsg, resp *Resp) os.Error {
	if w.superstep != 0 && w.superstep+1 != args.Superstep {
		*resp = NOT_OK
		return nil
	}
	// reset the outq to 0
	w.outq.reset()
	w.lastSuperstep = w.superstep
	w.lastCheckpoint = w.checkpoint

	// set up next step
	w.superstep = args.Superstep
	w.checkpoint = args.Checkpoint
	// collect info from master
	w.jobStats.numVertices = args.NumVerts
	*resp = OK
	w.logger.Printf("Firing step %d.  checkpoint = %t", w.superstep, w.checkpoint)
	go w.execStep()
	return nil
}

func (w *Worker) collectWorkerInfo() {
	w.workerStats.activeVertices = w.NumActiveVertices()
	w.workerStats.numVertices = w.NumVertices()
	w.workerStats.sentMsgs = w.NumSentMsgs()
}

func (w *Worker) execStep() os.Error {
	// XXX this is out of order?

	w.logger.Printf("Executing step %d", w.superstep)

	w.compute()
	w.logger.Printf("Done computation, flushing outq")
	// this blocks and will prevent us from notifying until all of our msgs are
	// sent
	w.outq.flush()

	// persist if checkpoint
	if w.checkpoint && w.persister != nil {
		// TODO: handle errors
		w.persister.Write(w)
	}

	w.collectWorkerInfo()

	w.logger.Printf("Step %d complete, notifying master", w.superstep)

	if err := w.notifyMaster("Master.NotifyStepComplete"); err != nil {
		return err
	}
	return nil
}

func (w *Worker) compute() os.Error {
	var wg sync.WaitGroup
	// XXX limit max routines?
	for _, p := range w.parts {
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

func (w *Worker) WriteResults(m *BasicMasterMsg, resp *Resp) os.Error {
	*resp = OK
	go w.writeResults()
	return nil
}

func (w *Worker) writeResults() {
	w.logger.Printf("Writing results")
	if w.resultWriter != nil {
		w.resultWriter.WriteResults(w)
	}
	w.notifyMaster("Master.NotifyWriteResultsComplete")
}

func (w *Worker) endJob(m *BasicMasterMsg, resp *Resp) os.Error {
	defer func() {
		w.mcl.Close()
		for wid, _ := range w.wmap {
			cl, _ := w.cl(wid)
			cl.Close()
		}
	}()
	*resp = OK
	return nil
}
