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

	phase int

	checkpointFn func(uint64) bool

	persister Persister
	loader    Loader

	client batter.MasterWorkerClient
}

func NewMaster(addr, port string) *Master {
	m := &Master{}

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

func newStepInfo() *stepInfo {
	return &stepInfo{
		Aggrs: make(map[string]interface{}),
	}
}

func collectStepData(collected *stepInfo, data *stepInfo) *stepInfo {
	collected.Active += data.Active
	collected.Total += data.Total
	collected.Sent += data.Sent
	return collected
}

func (m *Master) Compute() error {
	var lastCollected *stepInfo
	workers := m.Workers()
	for superstep := 0; ; superstep++ {
		grp := m.CreateTaskGroup("superstep/" + string(superstep))
		sendTaskToWorkers(workers, grp, func() batter.Task {
			return &SuperstepTask{
				Superstep: uint64(superstep),
				Aggrs:     lastCollected.Aggrs,
			}
		})
		m.FinishTaskGroup(grp)

		collected := newStepInfo()
		for _, aggr := range m.aggregators {
			aggr.Reset()
		}
		for resp := range grp.Response {
			resp := resp.(*SuperstepTaskResponse)
			if len(resp.Errors) > 0 {
				// XXX report errors
				panic("stuff failed")
				// return resp.Errors[0]
			}
			collected = collectStepData(collected, resp.Info)
			for name, val := range resp.Aggrs {
				if aggr, ok := m.aggregators[name]; ok {
					aggr.Submit(val)
				}
			}
		}

		failed := grp.Failures()
		if len(failed) > 0 {
			panic("stuff failed")
			// return NewPhaseFailureError(failed)
		}

		for name, aggr := range m.aggregators {
			collected.Aggrs[name] = aggr.ReduceAndEmit()
		}

		if collected.Active == 0 && collected.Sent == 0 {
			break
		}

		lastCollected = collected
	}

	return nil
}

func (m *Master) WriteResults() error {
	workers := m.Workers()
	grp := m.CreateTaskGroup("write")
	sendTaskToWorkers(workers, grp, func() batter.Task {
		return &WriteTask{}
	})
	m.FinishTaskGroup(grp)
	for resp := range grp.Response {
		resp := resp.(*WriteTaskResponse)
		if resp.Error != nil {
			panic("stuff failed")
		}
	}
	failed := grp.Failures()
	if len(failed) > 0 {
		panic("stuff failed")
	}
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
