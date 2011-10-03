package waffle

import (
	"http"
	"log"
	"os"
	"rpc"
	"net"
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

type Master struct {
	node
	jobId             string
	state             int
	regch             chan byte
	loadch            chan interface{}
	workch            chan interface{}
	writech           chan interface{}
	preparech         chan interface{}
	minWorkers        uint64
	registerWait      int64
	superstep         uint64
	partsPerWorker    uint64
	heartbeatInterval int64
	heartbeatTimeout  int64
	startTime         int64
	endTime           int64

	wInfo map[string]*workerInfo

	widFn func(string, string) string

	logger *log.Logger

	// job stats
	as          chan byte
	activeVerts uint64
	numVertices uint64
	sentMsgs    uint64
}

const (
	DEFAULT_HEARTBEAT_TIMEOUT = 3 * 1e9
	MREGISTER                 = iota
)

// For now, this is the barrier that the workers "enter" for sync
func (m *Master) barrier(ch chan interface{}) {
	bmap := make(map[string]interface{})
	for e := range ch {
		switch t := e.(type) {
		case string:
			bmap[t] = nil
		}
		if len(bmap) == len(m.wmap) {
			return
		}
	}
}

func NewMaster(addr, port, jobId string, minWorkers, partsPerWorker uint64, registerWait, heartbeatInterval, heartbeatTimeout int64) *Master {
	m := &Master{
		jobId:             jobId,
		regch:             make(chan byte, 1),
		as:                make(chan byte, 1),
		loadch:            make(chan interface{}),
		workch:            make(chan interface{}),
		writech:           make(chan interface{}),
		preparech:         make(chan interface{}),
		logger:            log.New(os.Stdout, "(master)["+net.JoinHostPort(addr, port)+"]["+jobId+"]: ", 0),
		minWorkers:        minWorkers,
		wInfo:             make(map[string]*workerInfo),
		partsPerWorker:    partsPerWorker,
		registerWait:      registerWait,
		heartbeatInterval: heartbeatInterval,
		heartbeatTimeout:  heartbeatTimeout,
	}
	m.InitNode(addr, port)
	m.regch <- 1
	m.as <- 1
	m.widFn = func(addr, port string) string {
		return net.JoinHostPort(addr, port)
	}
	return m
}

func (m *Master) InitMaster(addr, port string) {
	m.InitNode(addr, port)
}

// Zero out the stats from the last step
func (m *Master) clearActiveInfo() {
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
	m.partsPerWorker = partsPerWorker
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
	m.initialDataLoad()
	m.clearActiveInfo()
	m.secondaryDataLoad()

	return nil
}

func (m *Master) ekg(id string) {
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
		case <-time.Tick(m.heartbeatTimeout):
			// handle fault
		}

		// wait for the next interval
		select {
		case <-info.ekgch:
			return
		case <-time.Tick(m.heartbeatInterval):
			// resetTimeout
		}
	}
}

func (m *Master) Register(args *RegisterMsg, resp *RegisterResp) os.Error {
	m.logger.Printf("Worker is trying to register")
	<-m.regch
	defer func() { m.logger.Printf("Registration complete"); m.regch <- 1 }()
	/*
		if m.state != MREGISTER {
			(*resp).JobId = "" // should probably have a real error indicator
			return nil
		}
	*/
	wid := m.widFn(args.Addr, args.Port)
	// XXX For now, we don't care if someone registers twice, there will just be an overwrite
	resp.JobId = m.jobId
	resp.Wid = wid
	m.wmap[wid] = net.JoinHostPort(args.Addr, args.Port)
	m.wInfo[wid] = newWorkerInfo()
	go m.ekg(wid)
	m.logger.Printf("Registered: %s", wid)
	return nil
}

func (m *Master) registerWorkers() os.Error {
	m.logger.Printf("Registering workers")
	m.state = MREGISTER
	m.wmap = make(map[string]string)
	sec := int64(1 * 1e9)
	var timeout int64 = 0
	for uint64(len(m.wmap)) < m.minWorkers || timeout < m.registerWait {
		<-time.Tick(sec)
		timeout += sec
	}
	m.logger.Printf("registration complete")
	return nil
}

func (m *Master) determinePartitions() {
	m.logger.Printf("Designating partitions")

	pmap := make(map[uint64]string)
	var p uint64 = 0
	for id := range m.wmap {
		for i := 0; i < int(m.partsPerWorker); i++ {
			pmap[p] = id
			p++
		}
	}
	m.pmap = pmap

	m.logger.Printf("Done assigning %d partitions to %d workers", len(m.pmap), len(m.wmap))

	// Should be a seperate function/phase
	m.logger.Printf("Distributing worker and partition info")

	// XXX This is really not how topology should be distributed.  It might be better to have each worker download
	// a file from some location
	distch := make(chan interface{})
	tm := &ClusterInfoMsg{}
	tm.JobId = m.jobId
	tm.Pmap = m.pmap
	tm.Wmap = m.wmap
	if e := m.sendToAllWorkers("Worker.WorkerInfo", tm, distch); e != nil {
		panic(e.String())
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
				panic(e.String())
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
func (m *Master) initialDataLoad() os.Error {
	m.logger.Printf("Instructing workers to do first phase of data load")

	lm := &BasicMasterMsg{JobId: m.jobId}
	if e := m.sendToAllWorkers("Worker.DataLoad", lm, nil); e != nil {
		panic(e.String())
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

func (m *Master) secondaryDataLoad() os.Error {
	m.logger.Printf("Instructing workers to do second phase of data load")

	lm := &BasicMasterMsg{JobId: m.jobId}
	if e := m.sendToAllWorkers("Worker.SecondaryDataLoad", lm, nil); e != nil {
		panic(e.String())
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
	wr := &BasicMasterMsg{JobId: m.jobId}
	if e := m.sendToAllWorkers("Worker.WriteResults", wr, nil); e != nil {
		panic(e.String())
	}
	m.barrier(m.writech)

	m.logger.Printf("Workers have written")
	return nil
}

func (m *Master) NotifyWriteResultsComplete(args *WorkerInfoMsg, resp *Resp) os.Error {
	*resp = OK
	m.writech <- args.Wid
	return nil
}

func (m *Master) endWorkers() os.Error {
	if e := m.sendToAllWorkers("Worker.EndJob", &BasicMasterMsg{JobId: m.jobId}, nil); e != nil {
		panic(e.String())
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
	var msg BasicMasterMsg
	msg.JobId = m.jobId
	if e := m.sendToAllWorkers("Worker.PrepareForSuperstep", &msg, nil); e != nil {
		panic(e.String())
	}
	m.barrier(m.preparech)

	return nil
}

func (m *Master) NotifyPrepareComplete(args *WorkerInfoMsg, resp *Resp) os.Error {
	*resp = OK
	m.preparech <- args.Wid
	return nil
}

// super step
func (m *Master) execStep() os.Error {
	m.logger.Printf("doing step %d. active: %d. total: %d. sent: %d", m.superstep, m.activeVerts, m.numVertices, m.sentMsgs)
	sm := &SuperstepMsg{Superstep: m.superstep, NumVerts: m.numVertices}
	m.clearActiveInfo()
	sm.JobId = m.jobId
	for id := range m.wmap {
		wid := id
		cl, e := m.cl(wid)
		if e != nil {
			panic(e.String())
		}
		go func() {
			var r Resp
			if e := cl.Call("Worker.Superstep", sm, &r); e != nil {
				panic(e.String())
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
