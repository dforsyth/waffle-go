package waffle

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type MasterConfig struct {
	MinWorkers             uint64
	RegisterWait           int64
	MinPartitionsPerWorker uint64
	HeartbeatInterval      int64
	HeartbeatTimeout       int64
	MaxSteps               uint64
	JobId                  string
	StartStep              uint64
}

type phaseStatus struct {
	errors        []error
	failedWorkers []string // list of workers that have failed since phase started
}

func (s *phaseStatus) addError(wid string, err error) {
	if s.errors == nil {
		s.errors = make([]error, 0)
	}
	s.errors = append(s.errors, err)
}

type phaseInfo struct {
	activeVerts uint64
	numVerts    uint64
	sentMsgs    uint64
}

type jobInfo struct {
	phaseInfo      phaseInfo
	canRegister    bool
	lastCheckpoint uint64
	totalSentMsgs  uint64
}

type workerInfo struct {
	host          string
	port          string
	failed        bool
	lastHeartbeat int64
	heartbeatCh   chan byte
}

type Master struct {
	node

	Config  MasterConfig
	jobInfo jobInfo

	workerPool map[string]*workerInfo
	poolLock   sync.RWMutex

	currPhase int
	regch     chan byte
	barrierCh chan interface{}
	superstep uint64
	startTime int64
	endTime   int64

	checkpointFn func(uint64) bool

	phaseStatus phaseStatus
	ekgs        map[string]chan byte

	rpcServ   MasterRpcServer
	rpcClient MasterRpcClient
}

type barrierRemove struct {
	workerId string
	error    error
}

func (m *Master) EnterBarrier(summary *PhaseSummary) error {
	go func() {
		m.barrierCh <- summary
	}()
	return nil
}

func (m *Master) barrier(ch chan interface{}) {
	barrier := make(map[string]interface{})
	for hostPort := range m.workerPool {
		barrier[hostPort] = nil
	}
	if len(barrier) == 0 {
		log.Println("initial barrier map is empty!")
		return
	}
	for e := range ch {
		switch entry := e.(type) {
		case *PhaseSummary:
			if _, ok := barrier[entry.WorkerId]; ok {
				log.Printf("%s is entering the barrier", entry.WorkerId)
				m.collectSummaryInfo(entry)
				delete(barrier, entry.WorkerId)
			} else {
				log.Printf("%s is not in the barrier map, discarding PhaseSummary", entry.WorkerId)
			}
		case *barrierRemove:
			log.Printf("removing %s from barrier map", entry.workerId)
			delete(barrier, entry.workerId)
		}
		if len(barrier) == 0 {
			// barrier is empty, all of the workers we have been waiting for are accounted for
			return
		}
	}
}

func NewMaster(addr, port string) *Master {
	m := &Master{
		regch:     make(chan byte, 1),
		barrierCh: make(chan interface{}),
		ekgs:      make(map[string]chan byte),
	}

	m.InitNode(addr, port)
	m.regch <- 1
	m.checkpointFn = func(superstep uint64) bool {
		return false
	}
	return m
}

func (m *Master) SetCheckpointFn(fn func(uint64) bool) {
	m.checkpointFn = fn
}

// Zero out the stats from the last step
func (m *Master) resetPhaseInfo() {
	log.Println("resetting job info")
	m.jobInfo.phaseInfo.activeVerts = 0
	m.jobInfo.phaseInfo.sentMsgs = 0
	m.jobInfo.phaseInfo.numVerts = 0
}

// Update the stats from the current step
func (m *Master) collectSummaryInfo(ps *PhaseSummary) {
	if ps.Errors != nil && len(ps.Errors) > 0 {
		// if we find any errors in the summary, do not collect phase info data, just add to the recover info
		for _, err := range ps.Errors {
			m.phaseStatus.addError(ps.WorkerId, err)
		}
		return
	}
	m.jobInfo.phaseInfo.activeVerts += ps.ActiveVerts
	m.jobInfo.phaseInfo.numVerts += ps.NumVerts
	m.jobInfo.phaseInfo.sentMsgs += ps.SentMsgs
	m.jobInfo.totalSentMsgs += m.jobInfo.phaseInfo.sentMsgs
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
	return nil
}

func (m *Master) ekg(hostPort string, ekgch chan byte) {
	remote, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		panic("failed to resolve tcpaddr")
		// m.barrierCh <- &barrierRemove{workerId: id}
	}
	for {
		if conn, err := net.DialTCP("tcp", nil, remote); err != nil {
			log.Printf("worker %s could not be dialed", hostPort)
			m.barrierCh <- &barrierRemove{workerId: hostPort}
		} else {
			log.Printf("successful connect to %s", hostPort)
			conn.Close()
		}
		select {
		case <-time.After(m.Config.HeartbeatInterval):
		case <-ekgch:
			return // end the ekg loop
		}
	}
}

func (m *Master) RegisterWorker(host, port string) (string, error) {
	<-m.regch
	defer func() { m.regch <- 1 }()

	if !m.jobInfo.canRegister {
		// cant register, get out
		return "", errors.New("Registration is not open")
	}

	hostPort := net.JoinHostPort(host, port)

	log.Printf("Attempting to register %s", hostPort)

	if _, ok := m.workerPool[hostPort]; ok {
		log.Printf("%s already in the worker pool, replying with job id", hostPort)
		return m.Config.JobId, nil
	}
	m.workerPool[hostPort] = &workerInfo{
		host:          host,
		port:          port,
		failed:        false,
		lastHeartbeat: 0,
		heartbeatCh:   make(chan byte),
	}

	log.Printf("Registered %s:%s as %s for job %s", host, port, hostPort, m.Config.JobId)
	go m.ekg(hostPort, m.workerPool[hostPort].heartbeatCh)

	return m.Config.JobId, nil
}

func (m *Master) registerWorkers() error {
	log.Printf("Starting registration phase")

	m.workerPool = make(map[string]*workerInfo)

	// Should do this in a more Go-ish way, maybe with a select statement?
	m.jobInfo.canRegister = true
	for timer := 0; (m.Config.MinWorkers > 0 && uint64(len(m.workerPool)) < m.Config.MinWorkers) ||
		(m.Config.RegisterWait > 0 && int64(timer) < m.Config.RegisterWait); timer += 1 * 1e9 {
		<-time.After(1 * 1e9)
	}
	m.jobInfo.canRegister = false

	if len(m.workerPool) == 0 || uint64(len(m.workerPool)) < m.Config.MinWorkers && m.Config.RegisterWait > 0 {
		return errors.New("Not enough workers registered")
	}

	log.Printf("Registration phase complete")
	return nil
}

func (m *Master) determinePartitions() {
	log.Printf("Designating partitions")

	m.partitionMap = make(map[uint64]string)
	// XXX a better set of server configurations would allow us to set min partitions per worker.
	// They could send this information at registration time.
	for i, p := 0, 0; i < int(m.Config.MinPartitionsPerWorker); i++ {
		for hostPort := range m.workerPool {
			m.partitionMap[uint64(p)] = hostPort
			p++
		}
	}

	log.Printf("Assigned %d partitions to %d workers", len(m.partitionMap), len(m.workerPool))
}

func (m *Master) pushTopology() {
	log.Printf("Distributing topology information")

	topInfo := &TopologyInfo{JobId: m.Config.JobId, PartitionMap: m.partitionMap}
	var wg sync.WaitGroup
	for hostPort := range m.workerPool {
		hp := hostPort
		wg.Add(1)
		go func() {
			if err := m.rpcClient.PushTopology(hp, topInfo); err != nil {
				panic(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	log.Printf("Done distributing worker and partition info")
}

// Mark worker hostPort as failed
func (m *Master) markWorkerFailed(hostPort string) {
	m.poolLock.Lock()
	defer m.poolLock.Unlock()
	if info, ok := m.workerPool[hostPort]; ok {
		info.failed = true
	} else {
		log.Printf("cannot find %s in the worker pool to mark as failed", hostPort)
	}
}

// Workers that are marked as failed but still in the worker pool
func (m *Master) failedActiveWorkers() []*workerInfo {
	m.poolLock.RLock()
	defer m.poolLock.RUnlock()
	failed := make([]*workerInfo, 0)
	for _, info := range m.workerPool {
		if info.failed {
			failed = append(failed, info)
		}
	}
	return failed
}

func (m *Master) sendExecToAllWorkers(exec *PhaseExec) error {
	for hostPort := range m.workerPool {
		hp := hostPort
		go func() {
			if err := m.rpcClient.ExecutePhase(hp, exec); err != nil {
				m.markWorkerFailed(hp)
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
		NumVerts:   m.jobInfo.phaseInfo.numVerts,
		Checkpoint: m.checkpointFn(m.superstep),
	}
}

func (m *Master) executePhase(phaseId int) error {
	// Before we send out any orders to the workers, handle any failed workers that need to be removed from the pool

	// once that's taken care of, bump the phase and continue
	m.currPhase = phaseId
	m.resetPhaseInfo()
	if err := m.sendExecToAllWorkers(m.newPhaseExec(phaseId)); err != nil {
		return err
	}
	m.barrier(m.barrierCh)
	var numErrs = 0
	if m.phaseStatus.errors != nil {
		numErrs = len(m.phaseStatus.errors)
	}
	log.Printf("phase %d complete: %d active verticies, %d sent messages, %d errors", m.currPhase, m.jobInfo.phaseInfo.activeVerts,
		m.jobInfo.phaseInfo.sentMsgs, numErrs)

	// if any workers failed during the phase, the phase is invalid
	failedWorkers := m.failedActiveWorkers()
	if len(failedWorkers) > 0 {
		return errors.New("failed workers error") // NewPhaseFailureError("Failed Workers", failedWorkers)	
	}

	if m.phaseStatus.errors != nil && len(m.phaseStatus.errors) > 0 {
		// handle phase errors
		for error := range m.phaseStatus.errors {
			/*
				if err, ok := error.(RecoverablePhaseError); ok {
					// not really sure what the care for this would be...
				} else {
					panic("unrecoverable phase error...")
				}
			*/
			panic(error)
		}
	}

	return nil
}

func (m *Master) handleFailedWorkers(failedWorkers []string) error {
	for _, wid := range m.phaseStatus.failedWorkers {
		delete(m.workerPool, wid)
	}
	/*
		// register new workers, use recover timeout
		if m.Config.WaitForNewWorkers {
			if err := m.registerWorkers(); err != nil {
				// XXX we actually want to die gracefully, but until then just panic
				if toErr, ok := err.(*RegistrationTimeoutError); ok {
					log.Printf("registration timeout error, storing information")
					panic(toErr)
				} else {
					panic(err)
				}
				panic(err)
			}
		}
	*/
	if len(m.workerPool) == 0 {
		return errors.New("no workers left")
	}
	// Move the failed worker partitions to other workers
	for _, wid := range m.phaseStatus.failedWorkers {
		if err := m.movePartitions(wid); err != nil {
			return err
		}
	}
	return nil
}

// Move partitions of wid to another worker
func (m *Master) movePartitions(moveId string) error {
	// XXX for now, we just move the partitions for dead nodes to the first worker we get on map iteration.  Make this intelligent later.
	// Have this function return error so that we can fail if there is some kind of assignment overflow in the future heuristic
	var newOwner string
	for hostPort := range m.workerPool {
		if hostPort != moveId {
			newOwner = hostPort
			break
		}
	}
	for pid, wid := range m.partitionMap {
		if wid == moveId {
			log.Printf("moving partition %d from %s to %s", pid, moveId, newOwner)
			m.partitionMap[pid] = newOwner
		}
	}
	return nil
}

// run supersteps until there are no more active vertices or queued messages
func (m *Master) compute() error {
	log.Printf("Starting computation")

	log.Printf("Active verts = %d", m.jobInfo.phaseInfo.activeVerts)
	for m.superstep = 0; m.jobInfo.phaseInfo.activeVerts > 0 || m.jobInfo.phaseInfo.sentMsgs > 0; m.superstep++ {
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
func (m *Master) shutdownWorkers() error {
	for _, ch := range m.ekgs {
		ch <- 1
	}
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

	if m.Config.StartStep == 0 {
		m.executePhase(phaseLOAD1)
		m.executePhase(phaseLOAD2)
	} else {
		// This is a restart
		// Find the last checkpointed step for this job
		// Check that persisted data exists for that superstep, otherwise go to the next oldest checkpointed step
		// Tell workers to load data from that checkpoint
		// Redistribute vertices

		// rollback to the last checkpointed superstep
		m.superstep = m.Config.StartStep
		// load vertices from persistence
		m.executePhase(phaseLOAD3)
		// redistribute verts? (I think this is actually useless...)
		m.executePhase(phaseLOAD2)
		// set the superstep on workers
		m.executePhase(phaseRECOVER)
		// we should be ready to go now
	}

	m.startTime = time.Seconds()
	m.compute()
	m.endTime = time.Seconds()
	m.executePhase(phaseWRITE)
	log.Printf("compute time was %d", m.endTime-m.startTime)
	m.shutdownWorkers()
}
