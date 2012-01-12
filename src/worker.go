package waffle

import (
	"batter"
	"net"
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
	Active uint64
	Total  uint64
	Sent   uint64
	Aggrs  map[string]interface{}
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

	mClient batter.WorkerMasterClient
	wClient batter.WorkerWorkerClient

	done chan int
}

func NewWorker(addr, port string) *Worker {
	w := &Worker{
		partitions: make(map[uint64]*Partition),
	}

	w.initNode(addr, port)
	return w
}

func (w *Worker) WorkerId() string {
	return net.JoinHostPort(w.host, w.port)
}

func (w *Worker) Partitions() map[uint64]*Partition {
	return w.partitions
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

*/

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
