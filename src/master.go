package waffle

import (
	"log"
	"net"
	"os"
	"sort"
	"time"
	"sync"
)

type workerInfo struct {
	// wid string
	// addr string
	// port string
	ekgch chan byte
}

func newWorkerInfo() *workerInfo {
	return &workerInfo{
		ekgch: make(chan byte),
	}
}

type masterConfig struct {
	minWorkers        uint64
	registerWait      int64
	partsPerWorker    uint64
	heartbeatInterval int64
	heartbeatTimeout  int64
}

type Master struct {
	node

	config masterConfig

	jobId     string
	currPhase int
	regch     chan byte
	barrierCh chan *PhaseSummary
	superstep uint64
	startTime int64
	endTime   int64

	wInfo map[string]*workerInfo

	widFn        func(string, string) string
	checkpointFn func(uint64) bool

	// job stats
	mPhaseInfo  sync.RWMutex
	activeVerts uint64
	numVertices uint64
	sentMsgs    uint64

	rpcServ   MasterRpcServer
	rpcClient MasterRpcClient
}

func (m *Master) EnterBarrier(summary *PhaseSummary) os.Error {
	go func() {
		m.barrierCh <- summary
	}()
	return nil
}

// For now, this is the barrier that the workers "enter" for sync
func (m *Master) barrier(ch chan *PhaseSummary) {
	bmap := make(map[string]interface{})
	for ps := range ch {
		if m.jobId != ps.JobId {
			log.Fatalf("JobId mismatch in enterBarrier")
		}
		if m.currPhase != ps.PhaseId {
			log.Fatalf("Phase mismatch in enterBarrier from worker %s", ps.WorkerId)
		}

		m.collectSummaryInfo(ps)
		bmap[ps.WorkerId] = nil
		if len(bmap) == len(m.workerMap) {
			return
		}
	}
}

func NewMaster(addr, port, jobId string, minWorkers, partsPerWorker uint64, registerWait, heartbeatInterval, heartbeatTimeout int64) *Master {
	m := &Master{
		regch:     make(chan byte, 1),
		barrierCh: make(chan *PhaseSummary),
		wInfo:     make(map[string]*workerInfo),
	}

	m.jobId = jobId
	m.config.partsPerWorker = partsPerWorker
	m.config.registerWait = registerWait
	m.config.heartbeatInterval = heartbeatInterval
	m.config.heartbeatTimeout = heartbeatTimeout
	m.config.minWorkers = minWorkers

	m.InitNode(addr, port)
	m.regch <- 1
	m.widFn = func(addr, port string) string {
		return net.JoinHostPort(addr, port)
	}
	m.checkpointFn = func(superstep uint64) bool {
		return false
	}
	return m
}

func (m *Master) SetWorkerIdFn(fn func(string, string) string) {
	m.widFn = fn
}

func (m *Master) SetCheckpointFn(fn func(uint64) bool) {
	m.checkpointFn = fn
}

// Zero out the stats from the last step
func (m *Master) resetJobInfo() {
	// this doesnt even really need the locking -- it shouldnt happen while a barrier is accepting workers
	m.mPhaseInfo.Lock()
	m.activeVerts = 0
	m.sentMsgs = 0
	m.numVertices = 0
	m.mPhaseInfo.Unlock()
	log.Println("reset complete")
}

// Update the stats from the current step
func (m *Master) collectSummaryInfo(ps *PhaseSummary) {
	m.mPhaseInfo.Lock()
	m.activeVerts += ps.ActiveVerts
	m.numVertices += ps.NumVerts
	m.sentMsgs += ps.SentMsgs
	m.mPhaseInfo.Unlock()
}

func (m *Master) SetRpcClient(c MasterRpcClient) {
	m.rpcClient = c
}

func (m *Master) SetRpcServer(s MasterRpcServer) {
	m.rpcServ = s
}

// Init RPC
func (m *Master) init() os.Error {
	m.rpcServ.Start(m)
	m.rpcClient.Init()
	return nil
}

// Set partitions per worker
func (m *Master) SetPartitionsPerWorker(partsPerWorker uint64) {
	m.config.partsPerWorker = partsPerWorker
}

// job setup
func (m *Master) prepare() os.Error {
	// XXX Registration
	m.registerWorkers()
	m.determinePartitions()

	// Loading is a two step process: first we do the initial load by worker,
	// sending verts off to the correct worker if need be.  Then, we do a
	// second load step, where anything that was sent around is loaded.
	m.dataLoadPhase1()
	m.resetJobInfo()
	m.dataLoadPhase2()
	m.startTime = time.Seconds()

	return nil
}

func (m *Master) ekg(id string) {
	/*
		msg := &BasicMasterMsg{JobId: m.jobId}
		cl, e := m.cl(id)
		if e != nil {
			panic(e.String())
		}
		info := m.wInfo[id]
		var r Resp
		for {
			call := cl.Go("Worker.Healthcheck", msg, &r, nil)
			// return or timeout
			select {
			case <-call.Done:
				if call.Error != nil {
					panic(call.Error)
				}
			case <-time.After(m.config.heartbeatTimeout):
				// handle fault
			}

			// wait for the next interval
			select {
			case <-info.ekgch:
				return
			case <-time.Tick(m.config.heartbeatInterval):
				// resetTimeout
			}
		}
	*/
}

func (m *Master) RegisterWorker(addr, port string) (string, string, os.Error) {
	<-m.regch
	defer func() { m.regch <- 1 }()

	log.Printf("Attempting to register %s:%s", addr, port)

	workerId := m.widFn(addr, port)
	if _, ok := m.workerMap[workerId]; ok {
		log.Printf("%s already registered, overwriting")
	}
	m.workerMap[workerId] = net.JoinHostPort(addr, port)
	m.wInfo[workerId] = newWorkerInfo()

	jobId := m.jobId

	log.Printf("Registered %s:%s as %s for job %s", addr, port, workerId, jobId)
	go m.ekg(workerId)

	return workerId, jobId, nil
}

func (m *Master) registerWorkers() os.Error {
	log.Printf("Starting registration phase")

	m.workerMap = make(map[string]string)

	// Should do this in a more Go-ish way, maybe with a select statement?
	for timer := 0; uint64(len(m.workerMap)) < m.config.minWorkers || int64(timer) < m.config.registerWait; timer += 1 * 1e9 {
		<-time.After(1 * 1e9)
	}

	if len(m.workerMap) == 0 || uint64(len(m.workerMap)) < m.config.minWorkers && m.config.registerWait > 0 {
		return os.NewError("Not enough workers registered")
	}

	log.Printf("Registration phase complete")
	return nil
}

func (m *Master) determinePartitions() {
	log.Printf("Designating partitions")

	// iteration order undefined across platforms, pull out values and sort.
	workers := make([]string, 0, len(m.workerMap))
	for id := range m.workerMap {
		workers = append(workers, id)
	}
	sort.Strings(workers)

	m.partitionMap = make(map[uint64]string)
	p := 0
	for _, id := range workers {
		for i := 0; i < int(m.config.partsPerWorker); i, p = i+1, p+1 {
			m.partitionMap[uint64(p)] = id
		}
	}

	log.Printf("Assigned %d partitions to %d workers", len(m.partitionMap), len(m.workerMap))

	// Should be a seperate function/phase
	log.Printf("Distributing worker and partition information")

	var wg sync.WaitGroup
	for _, workerAddr := range workers {
		addr := workerAddr
		wg.Add(1)
		go func() {
			if err := m.rpcClient.PushTopology(addr,
				&TopologyInfo{JobId: m.jobId, PartitionMap: m.partitionMap, WorkerMap: m.workerMap}); err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	log.Printf("Done distributing worker and partition info")
}

func (m *Master) sendExecToAllWorkers(exec *PhaseExec) {
	for _, workerAddr := range m.workerMap {
		addr := workerAddr
		go func() {
			if err := m.rpcClient.ExecutePhase(addr, exec); err != nil {
				panic(err)
			}
		}()
	}
}

// Data loading is actually a two step phase: first, a worker will load the vertices from its designated
// data source and enter a barrier.  Once the barrier is full, the worker will be told to load vertices
// that were sent to it during the previous phase (because they did not belong on the worker that loaded
// them).
func (m *Master) dataLoadPhase1() os.Error {
	log.Printf("Instructing workers to do first phase of data load")

	m.currPhase = phaseLOAD1

	exec := &PhaseExec{PhaseId: phaseLOAD1}
	exec.JobId = m.jobId
	m.sendExecToAllWorkers(exec)
	m.barrier(m.barrierCh)

	log.Printf("Done first phase of data load")
	return nil
}

func (m *Master) dataLoadPhase2() os.Error {
	log.Printf("Instructing workers to do second phase of data load")

	exec := &PhaseExec{PhaseId: phaseLOAD2}
	exec.JobId = m.jobId
	m.sendExecToAllWorkers(exec)
	m.barrier(m.barrierCh)

	log.Printf("Done second phase of data load")
	return nil
}

// run supersteps until there are no more active vertices or queued messages
func (m *Master) compute() os.Error {
	log.Printf("Starting computation")

	log.Printf("Active verts = %d", m.activeVerts)
	for m.superstep = 0; m.activeVerts > 0 || m.sentMsgs > 0; m.superstep++ {
		// XXX prepareWorkers tells the worker to cycle message queues.  We should try to get rid of it.
		m.prepareWorkers()
		m.execStep()
	}

	log.Printf("Computation complete")
	return nil
}

// prepare workers for the next superstep
func (m *Master) prepareWorkers() os.Error {
	exec := &PhaseExec{
		PhaseId: phaseSTEPPREPARE,
	}
	exec.JobId = m.jobId

	m.sendExecToAllWorkers(exec)
	m.barrier(m.barrierCh)

	return nil
}

// superstep
func (m *Master) execStep() os.Error {
	log.Printf("Starting step %d -> (active: %d, total: %d, sent: %d)", m.superstep, m.activeVerts, m.numVertices, m.sentMsgs)

	exec := &PhaseExec{
		PhaseId:    phaseSUPERSTEP,
		Superstep:  m.superstep,
		NumVerts:   m.numVertices,
		Checkpoint: m.checkpointFn(m.superstep),
	}
	exec.JobId = m.jobId

	m.resetJobInfo()
	m.sendExecToAllWorkers(exec)

	m.barrier(m.barrierCh)

	return nil
}

// instruct workers to write results
func (m *Master) completeJob() os.Error {
	log.Printf("Instructing workers to write results")
	exec := &PhaseExec{PhaseId: phaseWRITE}
	m.sendExecToAllWorkers(exec)
	m.barrier(m.barrierCh)

	log.Printf("Workers have written results")
	return nil
}

// shutdown workers
func (m *Master) endWorkers() os.Error {
	/*
		if e := m.sendToAllWorkers("Worker.EndJob", &BasicMasterMsg{JobId: m.jobId}, nil); e != nil {
			panic(e)
		}
		// don't wait for a notify on this call	
		log.Printf("Killing ekgs and closing worker rpc clients")
		for wid, info := range m.wInfo {
			info.ekgch <- 1
			if cl, e := m.cl(wid); e == nil {
				cl.Close()
			}
		}
	*/
	return nil
}

// job finished
func (m *Master) finish() os.Error {
	m.completeJob()
	// m.releaseWorkers()
	m.endTime = time.Seconds()
	return nil
}

func (m *Master) Run() {
	m.init()
	m.prepare()
	m.compute()
	m.finish()
	log.Printf("Done")

	log.Printf("Job run time (post load) was %d seconds", m.endTime-m.startTime)
}
