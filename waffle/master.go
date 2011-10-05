package waffle

import (
	"http"
	"log"
	"net"
	"os"
	"rpc"
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

type masterConfig struct {
	minWorkers        uint64
	registerWait      int64
	partsPerWorker    uint64
	heartbeatInterval int64
	heartbeatTimeout  int64
	jobId             string
}

type Master struct {
	node

	config masterConfig

	state     int
	regch     chan byte
	loadch    chan interface{}
	workch    chan interface{}
	writech   chan interface{}
	preparech chan interface{}
	superstep uint64
	startTime int64
	endTime   int64

	wInfo map[string]*workerInfo

	widFn        func(string, string) string
	checkpointFn func(uint64) bool

	logger *log.Logger

	// job stats
	as          chan byte
	activeVerts uint64
	numVertices uint64
	sentMsgs    uint64
}

// For now, this is the barrier that the workers "enter" for sync
func (m *Master) barrier(ch chan interface{}) {
	bmap := make(map[string]interface{})
	for e := range ch {
		switch t := e.(type) {
		case string:
			bmap[t] = nil
		case os.Error:
			panic(t)
		}
		if len(bmap) == len(m.wmap) {
			return
		}
	}
}

func NewMaster(addr, port, jobId string, minWorkers, partsPerWorker uint64, registerWait, heartbeatInterval, heartbeatTimeout int64) *Master {
	m := &Master{
		regch:     make(chan byte, 1),
		as:        make(chan byte, 1),
		loadch:    make(chan interface{}),
		workch:    make(chan interface{}),
		writech:   make(chan interface{}),
		preparech: make(chan interface{}),
		logger:    log.New(os.Stdout, "(master)["+net.JoinHostPort(addr, port)+"]["+jobId+"]: ", 0),
		wInfo:     make(map[string]*workerInfo),
	}

	m.config.jobId = jobId
	m.config.partsPerWorker = partsPerWorker
	m.config.registerWait = registerWait
	m.config.heartbeatInterval = heartbeatInterval
	m.config.heartbeatTimeout = heartbeatTimeout
	m.config.minWorkers = minWorkers

	m.InitNode(addr, port)
	m.regch <- 1
	m.as <- 1
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
	<-m.as
	m.activeVerts = 0
	m.sentMsgs = 0
	m.numVertices = 0
	m.as <- 1
}

// Update the stats from the current step
func (m *Master) addActiveInfo(activeVerts, numVertices, sentMsgs uint64) {
	<-m.as
	m.activeVerts += activeVerts
	m.sentMsgs += sentMsgs
	m.numVertices += numVertices
	m.as <- 1
}

// Init RPC
func (m *Master) init() os.Error {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", net.JoinHostPort(m.addr, m.port))
	if e != nil {
		panic(e.String())
	}
	go http.Serve(l, nil)
	m.logger.Printf("Init complete")
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

	m.startTime = time.Seconds()
	m.determinePartitions()

	// Loading is a two step process: first we do the initial load by worker,
	// sending verts off to the correct worker if need be.  Then, we do a
	// second load step, where anything that was sent around is loaded.
	m.dataLoadPhase1()
	m.resetJobInfo()
	m.dataLoadPhase2()

	return nil
}

func (m *Master) ekg(id string) {
	msg := &BasicMasterMsg{JobId: m.config.jobId}
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
}

func (m *Master) Register(args *RegisterMsg, resp *RegisterResp) os.Error {
	// lock around the register operations
	<-m.regch
	defer func() { m.regch <- 1 }()

	addr, port := args.Addr, args.Port
	m.logger.Printf("Trying to register %s:%s", addr, port)

	// generate a unique worker id for this worker, then put the host information in the worker map
	wid := m.widFn(addr, port)
	m.wmap[wid] = net.JoinHostPort(addr, port)
	// create a worker info entry for this worker
	m.wInfo[wid] = newWorkerInfo()

	resp.JobId = m.config.jobId
	resp.Wid = wid

	// start running ekg on this worker
	go m.ekg(wid)

	m.logger.Printf("Registered %s:%s as %s", addr, port, wid)
	return nil
}

func (m *Master) registerWorkers() os.Error {
	m.logger.Printf("Starting registration phase")

	m.wmap = make(map[string]string)

	// Should do this in a more Go-ish way, maybe with a select statement?
	for timer := 0; uint64(len(m.wmap)) < m.config.minWorkers || int64(timer) < m.config.registerWait; timer += 1 * 1e9 {
		<-time.Tick(1 * 1e9)
	}

	if len(m.wmap) == 0 || uint64(len(m.wmap)) < m.config.minWorkers && m.config.registerWait > 0 {
		return os.NewError("Not enough workers registered")
	}

	m.logger.Printf("Registration phase complete")
	return nil
}

func (m *Master) determinePartitions() {
	m.logger.Printf("Designating partitions")

	m.pmap = make(map[uint64]string)
	p := 0
	for id := range m.wmap {
		for i := 0; i < int(m.config.partsPerWorker); i, p = i+1, p+1 {
			m.pmap[uint64(p)] = id
		}
	}

	m.logger.Printf("Assigned %d partitions to %d workers", len(m.pmap), len(m.wmap))

	// Should be a seperate function/phase
	m.logger.Printf("Distributing worker and partition information")

	// XXX This is really not how topology should be distributed.  It might be better to have each worker download
	// a file from some location
	msg := &ClusterInfoMsg{JobId: m.config.jobId, Pmap: m.pmap, Wmap: m.wmap}

	distch := make(chan interface{})
	if e := m.sendToAllWorkers("Worker.WorkerInfo", msg, distch); e != nil {
		panic(e)
	}
	m.barrier(distch)

	m.logger.Printf("Done distributing worker and partition info")
}

// Send a message to all of the workers we know of, using waitMap as our sync map.  If waitCh isn't nil, send the worker id on it
// once the rpc call is complete.
func (m *Master) sendToAllWorkers(call string, msg CoordMsg, waitCh chan interface{}) os.Error {
	for id := range m.wmap {
		wid := id
		cl, e := m.cl(wid)
		if e != nil {
			return e
		}
		go func() {
			var r Resp
			if e := cl.Call(call, msg, &r); e != nil {
				panic(e)
			}
			if r != OK {
				panic("Response from was not OK")
			}
			if waitCh != nil {
				waitCh <- wid
			}
		}()
	}

	return nil
}

// Data loading is actually a two step phase: first, a worker will load the vertices from its designated
// data source and enter a barrier.  Once the barrier is full, the worker will be told to load vertices
// that were sent to it during the previous phase (because they did not belong on the worker that loaded
// them).
func (m *Master) dataLoadPhase1() os.Error {
	m.logger.Printf("Instructing workers to do first phase of data load")

	msg := &BasicMasterMsg{JobId: m.config.jobId}
	if e := m.sendToAllWorkers("Worker.DataLoad", msg, nil); e != nil {
		panic(e)
	}
	m.barrier(m.loadch)

	m.logger.Printf("Done first phase of data load")
	return nil
}

func (m *Master) NotifyInitialDataLoadComplete(args *WorkerInfoMsg, resp *Resp) os.Error {
	*resp = OK
	go m.handleActiveInfoUpdate(args, m.loadch)
	return nil
}

func (m *Master) dataLoadPhase2() os.Error {
	m.logger.Printf("Instructing workers to do second phase of data load")

	msg := &BasicMasterMsg{JobId: m.config.jobId}
	if e := m.sendToAllWorkers("Worker.SecondaryDataLoad", msg, nil); e != nil {
		panic(e)
	}
	m.barrier(m.loadch)

	m.logger.Printf("Done second phase of data load")
	return nil
}

func (m *Master) NotifySecondaryDataLoadComplete(args *WorkerInfoMsg, resp *Resp) os.Error {
	*resp = OK
	go m.handleActiveInfoUpdate(args, m.loadch)
	return nil
}

// Deals with a WorkerInfoMsg
func (m *Master) handleActiveInfoUpdate(msg *WorkerInfoMsg, ch chan interface{}) {
	if !msg.Success {
		// handle computation errors
	} else {
		m.addActiveInfo(msg.ActiveVerts, msg.NumVerts, msg.SentMsgs)
	}
	ch <- msg.Wid
}

func (m *Master) completeJob() os.Error {
	m.logger.Printf("Instructing workers to write results")

	msg := &BasicMasterMsg{JobId: m.config.jobId}
	if e := m.sendToAllWorkers("Worker.WriteResults", msg, nil); e != nil {
		panic(e)
	}
	m.barrier(m.writech)

	m.logger.Printf("Workers have written results")
	return nil
}

func (m *Master) NotifyWriteResultsComplete(args *WorkerInfoMsg, resp *Resp) os.Error {
	*resp = OK
	m.writech <- args.Wid
	return nil
}

func (m *Master) endWorkers() os.Error {
	if e := m.sendToAllWorkers("Worker.EndJob", &BasicMasterMsg{JobId: m.config.jobId}, nil); e != nil {
		panic(e)
	}
	// don't wait for a notify on this call	
	m.logger.Printf("Killing ekgs and closing worker rpc clients")
	for wid, info := range m.wInfo {
		info.ekgch <- 1
		if cl, e := m.cl(wid); e == nil {
			cl.Close()
		}
	}
	return nil
}

// run the job
func (m *Master) compute() os.Error {
	m.logger.Printf("Starting computation")

	m.logger.Printf("Active verts = %d", m.activeVerts)
	for m.superstep = 0; m.activeVerts > 0 || m.sentMsgs > 0; m.superstep++ {
		// XXX prepareWorkers tells the worker to cycle message queues.  We should try to get rid of it.
		m.prepareWorkers()
		m.execStep()
	}

	m.logger.Printf("Computation complete")
	return nil
}

func (m *Master) prepareWorkers() os.Error {
	msg := &BasicMasterMsg{JobId: m.config.jobId}
	if e := m.sendToAllWorkers("Worker.PrepareForSuperstep", msg, nil); e != nil {
		panic(e)
	}
	m.barrier(m.preparech)

	return nil
}

func (m *Master) NotifyPrepareComplete(args *WorkerInfoMsg, resp *Resp) os.Error {
	*resp = OK
	go func() {
		m.preparech <- args.Wid
	}()
	return nil
}

// super step
func (m *Master) execStep() os.Error {
	m.logger.Printf("Starting step %d -> (active: %d, total: %d, sent: %d)", m.superstep, m.activeVerts, m.numVertices, m.sentMsgs)

	msg := &SuperstepMsg{
		JobId:      m.config.jobId,
		Superstep:  m.superstep,
		NumVerts:   m.numVertices,
		Checkpoint: m.checkpointFn(m.superstep),
	}

	m.resetJobInfo()
	for id := range m.wmap {
		wid := id
		cl, e := m.cl(wid)
		if e != nil {
			panic(e)
		}
		go func() {
			var r Resp
			if e := cl.Call("Worker.Superstep", msg, &r); e != nil {
				panic(e)
			}
			if r != OK {
				panic("Superstep reply was not ok")
			}
		}()
	}
	m.barrier(m.workch)

	return nil
}

func (m *Master) NotifyStepComplete(args *WorkerInfoMsg, resp *Resp) os.Error {
	*resp = OK
	go m.handleActiveInfoUpdate(args, m.workch)
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
	m.logger.Printf("Done")

	m.logger.Printf("Job run time (post registration) was %d seconds", m.endTime-m.startTime)
}
