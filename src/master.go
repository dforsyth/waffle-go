package waffle

import (
	"batter"
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
	MaxSteps               uint64
	JobId                  string
	StartStep              uint64
	LoadPaths              []string
}

type jobInfo struct {
	initialVerts   uint64
	loadedVerts    uint64
	canRegister    bool
	lastCheckpoint uint64
	totalSentMsgs  uint64
	startTime      int
	endTime        int
	started        bool
	superstep      uint64
}

type workerInfo struct {
	host          string
	port          string
	failed        bool
	errorMsg      string
	lastHeartbeat int
	heartbeatCh   chan byte
}

type Master struct {
	batter.Master

	node

	Config  MasterConfig
	jobInfo jobInfo

	workerPool map[string]*workerInfo
	poolLock   sync.RWMutex

	phase     int
	barrierCh chan barrierEntry

	checkpointFn func(uint64) bool

	persister Persister
	loader    Loader

	rpcServ   MasterRpcServer
	rpcClient MasterRpcClient
}

type barrierEntry interface {
	WorkerId() string
}

type barrierRemove struct {
	hostPort string
	error    error
}

func (e *barrierRemove) WorkerId() string {
	return e.hostPort
}

func (m *Master) EnterBarrier(summary PhaseSummary) error {
	go func() {
		m.barrierCh <- summary
	}()
	return nil
}

func (m *Master) barrier(ch chan barrierEntry) (collected []PhaseSummary, lost []string) {
	// create a map of workers to wait for using the current workerpool
	barrier := make(map[string]interface{})
	for hostPort := range m.workerPool {
		barrier[hostPort] = nil
	}
	if len(barrier) == 0 {
		log.Println("initial barrier map is empty!")
		return
	}
	// wait on the barrier channel
	for e := range ch {
		if _, ok := barrier[e.WorkerId()]; !ok {
			log.Printf("%s is not in the barrier map, discaring entry", e.WorkerId())
			continue
		}

		switch entry := e.(type) {
		case PhaseSummary:
			log.Printf("%s is entering the barrier", entry.WorkerId())
			collected = append(collected, entry)
		case *barrierRemove:
			log.Printf("removing %s from barrier map", entry.WorkerId())
			lost = append(lost, entry.hostPort)
		}
		delete(barrier, e.WorkerId())

		if len(barrier) == 0 {
			// barrier is empty, all of the workers we have been waiting for are accounted for
			return
		}
	}
	panic("not reached")
	return
}

func NewMaster(addr, port string) *Master {
	m := &Master{
		barrierCh: make(chan barrierEntry),
	}

	m.initNode(addr, port)
	m.checkpointFn = func(superstep uint64) bool {
		return false
	}

	// default configs
	m.Config.HeartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL
	m.Config.MaxSteps = DEFAULT_MAX_STEPS
	m.Config.MinPartitionsPerWorker = DEFAULT_MIN_PARTITIONS_PER_WORKER
	m.Config.MinWorkers = DEFAULT_MIN_WORKERS
	return m
}

func (m *Master) SetCheckpointFn(fn func(uint64) bool) {
	m.checkpointFn = fn
}

func (m *Master) SetRpcClient(c MasterRpcClient) {
	m.rpcClient = c
}

func (m *Master) SetRpcServer(s MasterRpcServer) {
	m.rpcServ = s
}

func (m *Master) SetPersister(p Persister) {
	m.persister = p
}

func (m *Master) SetLoader(loader Loader) {
	m.loader = loader
}

// Init RPC
func (m *Master) startRPC() error {
	m.rpcServ.Start(m)
	return nil
}

/*
func (m *Master) RegisterWorker(host, port string) (string, error) {
	m.poolLock.Lock()
	defer m.poolLock.Unlock()

	if !m.jobInfo.canRegister {
		// cant register, get out
		return "", errors.New("Registration is not open")
	}

	hostPort := net.JoinHostPort(host, port)

	log.Printf("Attempting to register %s", hostPort)

	if _, ok := m.workerPool[hostPort]; ok {
		// duplicate registration is okay
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
	go m.ekg(m.workerPool[hostPort])

	return m.Config.JobId, nil
}
*/

func (m *Master) registerWorkers() error {
	log.Printf("Starting registration phase")

	m.workerPool = make(map[string]*workerInfo)

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
	workers := m.Workers()
	for i, p := 0, 0; i < int(m.Config.MinPartitionsPerWorker); i++ {
		for _, hostPort := range workers {
			m.partitionMap[uint64(p)] = hostPort
			p++
		}
	}

	log.Printf("Assigned %d partitions to %d workers", len(m.partitionMap), len(workers))
}

func (m *Master) pushTopology() error {
	log.Printf("Distributing topology information")

	topInfo := &TopologyInfo{
		JobId:        m.Config.JobId,
		PartitionMap: m.partitionMap,
	}

	var wg sync.WaitGroup
	for hostPort := range m.workerPool {
		hp := hostPort
		wg.Add(1)
		go func() {
			if err := m.rpcClient.PushTopology(hp, topInfo); err != nil {
				m.markWorkerFailed(hp, err.Error())
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// TODO: check for failed workers

	log.Printf("Done distributing worker and partition info")
	return nil
}

// Mark worker hostPort as failed
func (m *Master) markWorkerFailed(hostPort, message string) {
	m.poolLock.Lock()
	defer m.poolLock.Unlock()

	if info, ok := m.workerPool[hostPort]; ok {
		log.Printf("marking %s as failed (%s)", hostPort, message)
		info.failed = true
		info.errorMsg = message
		m.barrierCh <- &barrierRemove{hostPort: hostPort}
	} else {
		log.Printf("cannot find %s in the worker pool to mark as failed (%s)", hostPort, message)
	}
}

func (m *Master) sendExecToAllWorkers(exec PhaseExec) {
	for hostPort := range m.workerPool {
		hp := hostPort
		go func() {
			if err := m.rpcClient.ExecutePhase(hp, exec); err != nil {
				m.markWorkerFailed(hp, err.Error())
			}
		}()
	}
}

func (m *Master) loadData() error {
	m.phase = PHASE_LOAD_DATA

	var workers []string
	for worker := range m.workerPool {
		workers = append(workers, worker)
	}
	exec := &LoadDataExec{
		LoadAssignments: m.loader.AssignLoad(workers, m.Config.LoadPaths),
	}
	exec.JId = m.Config.JobId
	exec.PId = m.phase

	summaries, lost := m.executePhase(exec)
	if lost != nil {
		panic("lost workers on load, need to repartition and try again...")
	}

	for _, summary := range summaries {
		if loadSummary, ok := summary.(*LoadDataSummary); ok {
			m.jobInfo.loadedVerts += loadSummary.LoadedVerts
		}
	}
	return nil
}

func (m *Master) loadRecievedVertices() error {
	m.phase = PHASE_DISTRIBUTE_VERTICES

	exec := &LoadRecievedExec{}
	exec.JId = m.Config.JobId
	exec.PId = m.phase

	summaries, lost := m.executePhase(exec)
	if lost != nil {

	}

	for _, summary := range summaries {
		if ldRecvSummary, ok := summary.(*LoadRecievedSummary); ok {
			m.jobInfo.initialVerts += ldRecvSummary.TotalVerts
		}
	}
	return nil
}

func (m *Master) stepPrepare() error {
	m.phase = PHASE_STEP_PREPARE

	exec := &StepPrepareExec{}
	exec.JId = m.Config.JobId
	exec.PId = m.phase

	_, lost := m.executePhase(exec)
	if lost != nil {

	}
	return nil
}

type stepStat struct {
	superstep                         uint64
	activeVerts, sentMsgs, totalVerts uint64
	aggregates                        map[string]interface{}
}

func (m *Master) superstep(last *stepStat) (*stepStat, error) {
	m.phase = PHASE_SUPERSTEP

	superstep := uint64(0)
	if last != nil {
		superstep = last.superstep + 1
	}
	log.Printf("--------- superstep %d ---------", superstep)
	exec := &SuperstepExec{
		Superstep:  superstep,
		Aggregates: make(map[string]interface{}),
	}
	exec.JId = m.Config.JobId
	exec.PId = m.phase

	if last != nil {
		for key, val := range last.aggregates {
			exec.Aggregates[key] = val
		}
	}
	for _, aggr := range m.aggregators {
		aggr.Reset()
	}

	summaries, lost := m.executePhase(exec)
	if lost != nil {

	}

	stepStats := &stepStat{
		superstep:  superstep,
		aggregates: make(map[string]interface{}),
	}
	for _, summary := range summaries {
		if stepSummary, ok := summary.(*SuperstepSummary); ok {
			stepStats.activeVerts += stepSummary.ActiveVerts
			stepStats.sentMsgs += stepSummary.SentMsgs
			stepStats.totalVerts += stepSummary.TotalVerts
			for key, val := range stepSummary.Aggregates {
				m.aggregators[key].Submit(val)
			}
		} else {
			log.Printf("got a bad summary type back")
		}
	}
	for key, aggr := range m.aggregators {
		stepStats.aggregates[key] = aggr.ReduceAndEmit()
	}
	log.Printf("------------ end %d -----------", superstep)
	return stepStats, nil
}

func (m *Master) compute() {
	var stats *stepStat
	var error error
	for {
		m.stepPrepare()
		stats, error = m.superstep(stats)
		if error != nil {
			panic(error)
		}
		if stats.activeVerts == 0 && stats.sentMsgs == 0 {
			log.Printf("activeVerts: %d, sentMsgs: %d", stats.activeVerts, stats.sentMsgs)
			break
		}
	}
}

func (m *Master) executePhase(exec PhaseExec) (summaries []PhaseSummary, lost []string) {
	/* 
	 * - check for failed workers, remove them from the pool and move their partitions to live workers
	 * - generate the phase execution order, and send it to all workers
	 * - throw up a barrier and wait
	 * - once the barrier constraints are met, check to see if any errors occured or if their were any workers lost in the phase.
	 * if there were, do not commit the phase info, otherwise commit.
	 */

	// Before we send out any orders to the workers, handle any failed workers that need to be removed from the pool
	m.purgeFailedWorkers()

	// for now, collect phase info on a per-phase basis and commit the info once the phase is verified successful.  in the future
	// it would be nice to collect this on a per-worker basis for fine grained stat collection and realtime resource allocation
	// info := newPhaseInfo()

	m.sendExecToAllWorkers(exec)
	// summaries, lost := m.barrier(m.barrierCh)

	return m.barrier(m.barrierCh)

	/*
			if lost != nil {
				m.purgeFailedWorkers(lost)
				error.lostWorkers = append(error.lostWorkers, lost...)
				return
			}

			// There are no errors occured during the phase, commit the phase info the overall job info
			log.Printf("phase %d complete: %d active verticies, %d sent messages, %d errors", m.phase, m.jobInfo.phaseInfo.activeVerts,
				m.jobInfo.phaseInfo.sentMsgs, len(info.errors))

			m.commitPhaseInfo(info)
		return nil
	*/
}

func (m *Master) purgeFailedWorkers() error {
	// XXX This is a really long lock...
	m.poolLock.Lock()
	defer m.poolLock.Unlock()
	// First, remove any workers marked as failed from the worker pool
	failedWorkers := make([]*workerInfo, 0)
	for hostPort, info := range m.workerPool {
		if info.failed {
			failedWorkers = append(failedWorkers, info)
			delete(m.workerPool, hostPort)
		}
	}

	// TODO: if we've dropped below minworkers, wait for more

	if len(m.workerPool) == 0 {
		return errors.New("no workers left")
	}
	// Move the failed worker partitions to other workers
	for _, info := range failedWorkers {
		hostPort := net.JoinHostPort(info.host, info.port)
		if err := m.movePartitions(hostPort); err != nil {
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

func (m *Master) writeResults() error {
	m.phase = PHASE_WRITE_RESULTS

	exec := &WriteResultsExec{}
	exec.JId = m.Config.JobId
	exec.PId = m.phase

	_, lost := m.executePhase(exec)
	if lost != nil {

	}
	return nil
}

// shutdown workers
func (m *Master) shutdownWorkers() error {
	for _, info := range m.workerPool {
		info.heartbeatCh <- 1
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

type LoadTask struct {
	batter.TaskerBase
	w            *Worker
	PartitionMap map[uint64]string
	Assignment   map[string][]string // maybe just bring this down to a []string.  does every worker really need to know what the other loaded?
}

type LoadTaskResponse struct {
	batter.TaskerBase
	Errors      []error
	TotalLoaded uint64
}

func (t *LoadTask) Execute() (batter.TaskResponse, error) {
	// set the partition map
	t.w.partitionMap = t.PartitionMap

	var assigned []string
	var ok bool
	// w is set on the way in
	if assigned, ok = t.Assignment[t.w.WorkerId()]; !ok {
		log.Printf("no load assignments for %s", t.w.WorkerId())
		return &LoadTaskResponse{}, nil
	}
	var totalLoaded uint64
	for _, assignment := range assigned {
		loaded, err := t.w.loader.Load(t.w, assignment)
		if err != nil {
			return &LoadTaskResponse{Errors: []error{err}}, nil
		}
		totalLoaded += loaded
	}

	return &LoadTaskResponse{TotalLoaded: totalLoaded}, nil
}

func (m *Master) PartitionAndLoad() error {
	m.determinePartitions()
}

func (m *Master) Start() {
	// m.startRPC()
	go m.Run()
	m.WaitForWorkers(m.Config.MinWorkers)
	m.DisableRegistration()

	m.PartitionAndLoad()

	m.registerWorkers()
	m.determinePartitions()
	m.pushTopology()

	m.loadData()
	m.loadRecievedVertices()
	m.jobInfo.startTime = time.Now().Nanosecond()
	m.compute()
	m.jobInfo.endTime = time.Now().Nanosecond()

	m.writeResults()
	log.Printf("compute time was %d", m.jobInfo.endTime-m.jobInfo.startTime)
	log.Printf("total sent messages was %d", m.jobInfo.totalSentMsgs)
	m.shutdownWorkers()
}
