package waffle

import (
	"batter"
	"log"
	"sync"
)

type MasterConfig struct {
	Host                   string
	Port                   string
	MinWorkers             int
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

	client batter.MasterWorkerClient
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

func (m *Master) SetPersister(p Persister) {
	m.persister = p
}

func (m *Master) SetLoader(loader Loader) {
	m.loader = loader
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

/*

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
*/

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

func (m *Master) PartitionAndLoad() error {
	grp := m.CreateTaskGroup("load")

	m.determinePartitions()

	workers := m.Workers()
	assignments := m.loader.AssignLoad(workers, m.Config.LoadPaths)
	sendTaskToWorkers(workers, grp, func() batter.Task {
		return &LoadTask{
			PartitionMap: m.partitionMap,
			Assignments:  assignments,
		}
	})
	m.FinishTaskGroup(grp)

	var totalLoaded uint64
	for resp := range grp.Response {
		resp := resp.(*LoadTaskResponse)
		if resp.Errors != nil || len(resp.Errors) > 0 {
			// TODO: handle errors
		}
		totalLoaded += resp.TotalLoaded
	}
	log.Printf("Loaded %d vertices", totalLoaded)
	/*
		if failed := grp.Failures(); len(failed) > 0 {
			return NewFailedTasksError(failed)
		}
	*/
	return nil
}

func (m *Master) Compute() error {
	workers := m.Workers()
	for superstep := 0; ; superstep++ {
		grp := m.CreateTaskGroup("superstep/" + string(superstep))
		sendTaskToWorkers(workers, grp, func() batter.Task {
			return &SuperstepTask{}
		})
		m.FinishTaskGroup(grp)

		for resp := range grp.Response {
			_ = resp.(*SuperstepTaskResponse)

		}
		/*
			if active == 0 && sent == 0 {
				break
			}
		*/
	}

	return nil
}

func (m *Master) WriteResults() error {
	// workers := m.Workers()
	return nil
}

// helper to fire off tasks
func sendTaskToWorkers(workers []string, grp *batter.TaskGroup, taskGen func() batter.Task) {
	for _, worker := range workers {
		task := taskGen()
		task.SetWorkerId(worker)
		grp.Send <- task
	}
}

func (m *Master) Start() {
	m.Init(m.Config.Host, m.Config.Port, m.client)
	m.Run()
	m.WaitForWorkers(m.Config.MinWorkers)
	m.CloseRegistration()

	m.PartitionAndLoad()
	m.Compute()
	m.WriteResults()
}
