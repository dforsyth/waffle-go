package waffle

import (
	"donut"
	"encoding/json"
	"errors"
	"gozk"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"path"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	NewState = iota
	SetupState
	PrepareState
	LoadState
	RunState
	WriteState
)

type Coordinator struct {
	// workers
	workers *donut.SafeMap

	// config for waffle
	config *Config

	// graph partition on this node
	// TODO: make this a map of partition to graph so that we can pick up partitions from failed workers
	graph *Graph

	zk                                            *gozk.ZooKeeper
	watchers                                      map[string]chan byte
	basePath, lockPath, barriersPath, workersPath string

	state       int32
	clusterName string
	// needed for CreateWork
	donutConfig      *donut.Config
	partitions       map[int]string
	cachedWorkerInfo map[string]map[string]interface{}

	rpcClients map[string]*rpc.Client

	done chan byte
}

func newCoordinator(clusterName string, c *Config) *Coordinator {
	return &Coordinator{
		clusterName: clusterName,
		state:       NewState,
		config:      c,
		watchers:    make(map[string]chan byte),
		partitions:  make(map[int]string),
		workers:     donut.NewSafeMap(nil),
		rpcClients:  make(map[string]*rpc.Client),
	}
}

func (c *Coordinator) createPaths() {
	c.basePath = path.Join("/", c.config.JobId)
	c.lockPath = path.Join(c.basePath, LockPath)
	c.workersPath = path.Join(c.basePath, WorkersPath)
	c.barriersPath = path.Join(c.basePath, BarriersPath)

	c.zk.Create(c.basePath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(c.workersPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(c.barriersPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
}

func (c *Coordinator) setup() {
	// create the paths for this job
	c.createPaths()
	// start rpc server
	c.startServer()
	// watch the workers path
	watchZKChildren(c.zk, c.workersPath, c.workers, func(m *donut.SafeMap) {
		c.onWorkersChange(m)
	})
}

func (c *Coordinator) startServer() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", net.JoinHostPort(c.config.RPCHost, c.config.RPCPort))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) SubmitVertex(v Vertex, r *int) error {
	c.graph.addVertex(v)
	*r = 0
	return nil
}

func (c *Coordinator) sendVertex(v Vertex, pid int) error {
	w := c.partitions[pid]
	cl := c.rpcClients[w]
	var r int
	return cl.Call("Coordinator.SubmitVertex", &v, &r)
}

func (c *Coordinator) SubmitEdge(e Edge, r *int) error {
	c.graph.addEdge(e)
	*r = 0
	return nil
}

func (c *Coordinator) sendEdge(e Edge, pid int) error {
	w := c.partitions[pid]
	cl := c.rpcClients[w]
	var r int
	return cl.Call("Coordinator.SubmitEdge", &e, &r)
}

func (c *Coordinator) SubmitMessage(m Message, r *int) error {
	c.graph.addMessage(m)
	*r = 0
	return nil
}

func (c *Coordinator) sendMessage(m Message, pid int) error {
	w := c.partitions[pid]
	cl := c.rpcClients[w]
	var r int
	return cl.Call("Coordinator.SubmitMessage", &m, &r)
}

func (c *Coordinator) register() {
	for {
		if _, err := c.zk.Create(c.lockPath, "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
			defer c.zk.Delete(c.lockPath, -1)
			if c.workers.Len() < c.config.InitialWorkers {
				info := c.info()
				if _, err := c.zk.Create(path.Join(c.workersPath, c.config.NodeId), info, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
					log.Fatalln(err)
				}
				return
			}
			log.Fatalln("InitialWorkers has been met for this job, exiting")
		}
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) createBarrier(name string, onChange func(*donut.SafeMap)) {
	bPath := path.Join(c.barriersPath, name)
	if _, ok := c.watchers[bPath]; !ok {
		if _, err := c.zk.Create(bPath, "", 0, gozk.WorldACL(gozk.PERM_ALL)); err == nil {
			log.Printf("Created barrier %s", bPath)
		} else {
			log.Printf("Failed to create barrier %s: %v", bPath, err)
		}
		kill, err := watchZKChildren(c.zk, bPath, donut.NewSafeMap(make(map[string]interface{})), onChange)
		if err != nil {
			log.Fatalln(err)
		}
		c.watchers[name] = kill
	}
}

func (c *Coordinator) enterBarrier(name, entry, data string) {
	log.Printf("Entering barrier %s as %s", name, entry)
	if _, err := c.zk.Create(path.Join(c.barriersPath, name, entry), data, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
		log.Fatalf("Error on barrier entry (%s entering %s): %v", entry, name, err)
	}
}

func (c *Coordinator) start(zk *gozk.ZooKeeper) error {
	if !atomic.CompareAndSwapInt32(&c.state, NewState, SetupState) {
		return errors.New("Error moving from NewState to SetupState")
	}
	c.zk = zk
	c.setup()
	c.register()
	return nil
}

func (c *Coordinator) startWork(workId string, data map[string]interface{}) {
	switch data["work"].(string) {
	case "load":
		p := data["path"].(string)
		c.graph.Load(p)
		c.enterBarrier("load", p, "")
	case "superstep":
		step := int(data["step"].(float64))

		c.createBarrier("superstep-"+strconv.Itoa(step), func(m *donut.SafeMap) {
			c.onStepBarrierChange(step, m)
		})

		log.Printf("Running superstep %d", step)
		stepData := make(map[string]interface{})
		stepData["active"], stepData["msgs"], stepData["aggr"] = c.graph.runSuperstep(step)
		log.Printf("Step %d stats: %d active verts, %d sent messages", step, stepData["active"], stepData["msgs"])

		data, _ := json.Marshal(stepData)
		c.enterBarrier("superstep-"+strconv.Itoa(step), c.config.NodeId, string(data))
	case "write":
		c.createBarrier("write", func(m *donut.SafeMap) {
			c.onWriteBarrierChange(m)
		})
		if err := c.graph.Write(); err != nil {
			panic(err)
		}
		c.enterBarrier("write", c.config.NodeId, "")
	}
}

func (c *Coordinator) onStepBarrierChange(step int, m *donut.SafeMap) {
	if m.Len() == c.workers.Len() {
		defer m.Clear()
		barrierName := "superstep-" + strconv.Itoa(step)
		// the barrier is full, collect information and launch the next step
		c.graph.globalStat.reset()
		c.graph.globalStat.step = step
		// collect and unmarshal data for all entries in the barrier
		lm := m.GetCopy()
		for k := range lm {
			if data, _, err := c.zk.Get(path.Join(c.barriersPath, barrierName, k)); err == nil {
				var info map[string]interface{}
				if err := json.Unmarshal([]byte(data), &info); err != nil {
					panic(err)
				}
				c.graph.globalStat.active += int(info["active"].(float64))
				c.graph.globalStat.msgs += int(info["msgs"].(float64))
			} else {
				panic(err)
			}
		}
		// kill the watcher on this barrier
		c.watchers[barrierName] <- 1
		delete(c.watchers, barrierName)
		if c.graph.globalStat.active == 0 && c.graph.globalStat.msgs == 0 {
			atomic.StoreInt32(&c.state, WriteState)
			go c.createWriteWork()
		} else {
			go c.createStepWork(step + 1)
		}
	} else {
		log.Printf("step barrier change: %d entries out of %d", m.Len(), c.workers.Len())
	}
}

func (c *Coordinator) onWorkersChange(m *donut.SafeMap) {
	log.Println("workers updated")
	if atomic.LoadInt32(&c.state) > SetupState {
		// invalidate current step
		// update partition mapping
		// roll back to last checkpoint
	} else {
		if m.Len() == c.config.InitialWorkers {
			// go into prepare state
			if !atomic.CompareAndSwapInt32(&c.state, SetupState, PrepareState) {
				log.Println("Could not properly move from SetupState to PrepareState")
				return
			}
			log.Printf("InitialWorkers met, preparing node for work")
			// everyone is here, create the partition mapping
			lm := m.RangeLock()
			var workers []string
			for k := range lm {
				workers = append(workers, k)
			}
			m.RangeUnlock()
			sort.Strings(workers)
			for i := 0; i < len(workers); i++ {
				c.partitions[i] = workers[i]
				if workers[i] == c.config.NodeId {
					c.graph.partitionId = i
				}
			}

			// set up connections to all the other nodes
			c.cachedWorkerInfo = make(map[string]map[string]interface{})
			c.rpcClients = make(map[string]*rpc.Client)
			for _, w := range workers {
				// pull down worker info for all of the existing workers
				c.cachedWorkerInfo[w] = c.workerInfo(w)
				c.rpcClients[w], _ = rpc.DialHTTP("tcp", net.JoinHostPort(c.cachedWorkerInfo[w]["host"].(string), c.cachedWorkerInfo[w]["port"].(string)))
			}

			// go into loadstate
			if !atomic.CompareAndSwapInt32(&c.state, PrepareState, LoadState) {
				log.Println("Could not properly move from PrepareState to LoadState")
				return
			}
			go c.createLoadWork()
		}
	}
}

func (c *Coordinator) info() string {
	m := make(map[string]interface{})
	m["host"] = c.config.RPCHost
	m["port"] = c.config.RPCPort

	info, _ := json.Marshal(m)
	return string(info)
}

func (c *Coordinator) workerInfo(id string) (info map[string]interface{}) {
	raw, _, err := c.zk.Get(path.Join(c.workersPath, id))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		panic(err)
	}
	return info
}

func (c *Coordinator) onLoadBarrierChange(m *donut.SafeMap) {
	if m.Len() == len(c.graph.job.LoadPaths()) {
		log.Printf("load complete")
		c.watchers["load"] <- 1
		delete(c.watchers, "load")
		if !atomic.CompareAndSwapInt32(&c.state, LoadState, RunState) {
			log.Println("Could not properly move from LoadState to RunState")
			return
		}
		go c.createStepWork(1)
	} else {
		log.Printf("Load barrier has %d/%d entries", m.Len(), len(c.graph.job.LoadPaths()))
	}
}

func (c *Coordinator) onWriteBarrierChange(m *donut.SafeMap) {
	if m.Len() == c.workers.Len() {
		log.Println("Write barrier full, ending job")
		c.done <- 1
	}
}

func (c *Coordinator) createWriteWork() {
	log.Printf("creating work for write %s", c.config.NodeId)
	data := make(map[string]interface{})
	data[c.clusterName] = c.config.NodeId
	data["work"] = "write"
	donut.CreateWork(c.clusterName, c.zk, c.donutConfig, "write-"+c.config.NodeId, data)
}

func (c *Coordinator) createLoadWork() {
	log.Println("creating load work")
	data := make(map[string]interface{})
	data["work"] = "load"
	paths := c.graph.job.LoadPaths()
	// create the load barrier here since a node might not end up with load work
	c.createBarrier("load", func(m *donut.SafeMap) {
		c.onLoadBarrierChange(m)
	})
	for _, p := range paths {
		data["path"] = p
		workName := "load-" + p
		donut.CreateWork(c.clusterName, c.zk, c.donutConfig, workName, data)
	}
}

func (c *Coordinator) createStepWork(step int) {
	log.Printf("creating work for superstep %d", step)
	data := make(map[string]interface{})
	data[c.clusterName] = c.config.NodeId
	data["work"] = "superstep"
	data["step"] = step
	donut.CreateWork(c.clusterName, c.zk, c.donutConfig, "superstep-"+strconv.Itoa(step)+"-"+c.config.NodeId, data)
}
