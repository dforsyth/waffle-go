package waffle

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
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

type MasterConfig struct {
	MinWorkers          uint64
	RegisterWait        int64
	PartitionsPerWorker uint64
	HeartbeatInterval   int64
	HeartbeatTimeout    int64
	MaxSteps            uint64
	JobId               string
}

type Master struct {
	node

	Config MasterConfig

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

func (m *Master) EnterBarrier(summary *PhaseSummary) error {
	go func() {
		m.barrierCh <- summary
	}()
	return nil
}

// For now, this is the barrier that the workers "enter" for sync
func (m *Master) barrier(ch chan *PhaseSummary) {
	bmap := make(map[string]interface{})
	for ps := range ch {
		if m.Config.JobId != ps.JobId {
			log.Fatalf("JobId mismatch in enterBarrier")
		}
		if m.currPhase != ps.PhaseId {
			log.Fatalf("Phase mismatch in enterBarrier from worker %s", ps.WorkerId)
		}

		if ps.Error != nil {
			// handle error
		}
		m.collectSummaryInfo(ps)
		bmap[ps.WorkerId] = nil
		if len(bmap) == len(m.workerMap) {
			return
		}
	}
}

func NewMaster(addr, port string) *Master {
	m := &Master{
		regch:     make(chan byte, 1),
		barrierCh: make(chan *PhaseSummary),
		wInfo:     make(map[string]*workerInfo),
	}

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
	log.Println("resetting job info")
	m.mPhaseInfo.Lock()
	m.activeVerts = 0
	m.sentMsgs = 0
	m.numVertices = 0
	m.mPhaseInfo.Unlock()
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
func (m *Master) startRPC() error {
	m.rpcServ.Start(m)
	m.rpcClient.Init()
	return nil
}

func (m *Master) ekg(id string) {
	/*
		msg := &BasicMasterMsg{JobId: m.Config.JobId}
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

func (m *Master) RegisterWorker(addr, port string) (string, string, error) {
	<-m.regch
	defer func() { m.regch <- 1 }()

	log.Printf("Attempting to register %s:%s", addr, port)

	workerId := m.widFn(addr, port)
	if _, ok := m.workerMap[workerId]; ok {
		log.Printf("%s already registered, overwriting")
	}
	m.workerMap[workerId] = net.JoinHostPort(addr, port)
	m.wInfo[workerId] = newWorkerInfo()

	jobId := m.Config.JobId

	log.Printf("Registered %s:%s as %s for job %s", addr, port, workerId, jobId)
	go m.ekg(workerId)

	return workerId, jobId, nil
}

func (m *Master) registerWorkers() error {
	log.Printf("Starting registration phase")

	m.workerMap = make(map[string]string)

	// Should do this in a more Go-ish way, maybe with a select statement?
	for timer := 0; uint64(len(m.workerMap)) < m.Config.MinWorkers || int64(timer) < m.Config.RegisterWait; timer += 1 * 1e9 {
		<-time.After(1 * 1e9)
	}

	if len(m.workerMap) == 0 || uint64(len(m.workerMap)) < m.Config.MinWorkers && m.Config.RegisterWait > 0 {
		return errors.New("Not enough workers registered")
	}

	log.Printf("Registration phase complete")
	return nil
}

func (m *Master) determinePartitions() {
	log.Printf("Designating partitions")

	m.partitionMap = make(map[uint64]string)
	p := 0
	for _, id := range m.workerMap {
		for i := 0; i < int(m.Config.PartitionsPerWorker); i, p = i+1, p+1 {
			m.partitionMap[uint64(p)] = id
		}
	}

	log.Printf("Assigned %d partitions to %d workers", len(m.partitionMap), len(m.workerMap))
}

func (m *Master) pushTopology() {
	log.Printf("Distributing worker and partition information")

	topInfo := &TopologyInfo{JobId: m.Config.JobId, PartitionMap: m.partitionMap, WorkerMap: m.workerMap}
	var wg sync.WaitGroup
	for _, workerAddr := range m.workerMap {
		addr := workerAddr
		wg.Add(1)
		go func() {
			if err := m.rpcClient.PushTopology(addr, topInfo); err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	log.Printf("Done distributing worker and partition info")
}

func (m *Master) sendExecToAllWorkers(exec *PhaseExec) error {
	for _, workerAddr := range m.workerMap {
		addr := workerAddr
		go func() {
			if err := m.rpcClient.ExecutePhase(addr, exec); err != nil {
				panic(err)
			}
		}()
	}
	return nil
}

func (m *Master) newPhaseExec(phaseId int) *PhaseExec {
	return &PhaseExec{
		PhaseId:    phaseId,
		JobId:      m.Config.JobId,
		Superstep:  m.superstep,
		NumVerts:   m.numVertices,
		Checkpoint: m.checkpointFn(m.superstep),
	}
}

func (m *Master) executePhase(phaseId int) error {
	m.currPhase = phaseId
	m.resetJobInfo()
	if err := m.sendExecToAllWorkers(m.newPhaseExec(phaseId)); err != nil {
		return err
	}
	if m.currPhase != phaseSHUTDOWN {
		m.barrier(m.barrierCh)
	}
	return nil
}

// run supersteps until there are no more active vertices or queued messages
func (m *Master) compute() error {
	log.Printf("Starting computation")

	log.Printf("Active verts = %d", m.activeVerts)
	for m.superstep = 0; m.activeVerts > 0 || m.sentMsgs > 0; m.superstep++ {
		// XXX prepareWorkers tells the worker to cycle message queues.  We should try to get rid of it.
		log.Printf("preparing for superstep %d", m.superstep)
		m.executePhase(phaseSTEPPREPARE)
		log.Printf("starting superstep %d", m.superstep)
		m.executePhase(phaseSUPERSTEP)
	}

	log.Printf("Computation complete")
	return nil
}

// shutdown workers
func (m *Master) endWorkers() error {
	/*
		if e := m.sendToAllWorkers("Worker.EndJob", &BasicMasterMsg{JobId: m.Config.JobId}, nil); e != nil {
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

func (m *Master) Run() {
	m.startRPC()

	m.registerWorkers()
	m.determinePartitions()
	m.pushTopology()

	m.executePhase(phaseLOAD1)
	m.executePhase(phaseLOAD2)
	m.startTime = time.Seconds()
	m.compute()
	m.endTime = time.Seconds()
	m.executePhase(phaseWRITE)
	log.Printf("compute time was %d", m.endTime-m.startTime)
	m.executePhase(phaseSHUTDOWN)
}
