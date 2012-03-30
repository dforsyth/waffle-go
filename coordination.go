package waffle

import (
	"donut"
	"encoding/json"
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
	graph *Graph

	zk                                            *gozk.ZooKeeper
	barrierCount                                  int
	barrierMap                                    *donut.SafeMap
	watchers                                      map[string]chan byte
	basePath, lockPath, barriersPath, workersPath string

	state       int32
	clusterName string
	donutConfig *donut.Config
	partitions  map[int]string
	workerInfos map[string]map[string]interface{}

	rpcClients map[string]*rpc.Client
	host, port string

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

func (c *Coordinator) setup() {
	if !atomic.CompareAndSwapInt32(&c.state, NewState, SetupState) {
		log.Fatal("Could not swap to setup state to begin setup")
	}
	c.basePath = path.Join("/", c.config.JobId)
	c.lockPath = path.Join(c.basePath, "lock")
	c.workersPath = path.Join(c.basePath, "workers")
	c.barriersPath = path.Join(c.basePath, "barriers")

	c.zk.Create(c.basePath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(c.workersPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(c.barriersPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))

	// start servers
	c.startServer()

	watchZKChildren(c.zk, c.workersPath, c.workers, func(m *donut.SafeMap) {
		c.onWorkersChange(m)
	})
}

func (c *Coordinator) startServer() {
	rpc.Register(c)
	rpc.HandleHTTP()
	c.host = "localhost"
	c.port = "500" + c.config.NodeId
	l, e := net.Listen("tcp", net.JoinHostPort(c.host, c.port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) SubmitVertex(v Vertex, r *int) error {
	// log.Printf("got %s", v.Id())
	c.graph.addVertex(v)
	*r = 0
	return nil
}

func (c *Coordinator) sendVertex(v Vertex, pid int) error {
	w := c.partitions[pid]
	cl := c.rpcClients[w]
	var r int
	// log.Printf("sending %s", v.Id())
	return cl.Call("Coordinator.SubmitVertex", &v, &r)
}

func (c *Coordinator) SubmitEdge(e Edge, r *int) error {
	// log.Printf("got edge %s-%s", e.Source(), e.Destination())
	c.graph.addEdge(e)
	*r = 0
	return nil
}

func (c *Coordinator) sendEdge(e Edge, pid int) error {
	w := c.partitions[pid]
	cl := c.rpcClients[w]
	var r int
	// log.Printf("sending edge %s-%s", e.Source(), e.Destination())
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
	log.Println("register")
	for {
		log.Printf("len: %d", c.workers.Len())
		m := c.workers.RangeLock()
		for k := range m {
			log.Printf("k: %s", k)
		}
		c.workers.RangeUnlock()
		if _, err := c.zk.Create(c.lockPath, "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
			defer c.zk.Delete(c.lockPath, -1)
			if c.workers.Len() < c.config.InitialWorkers {
				info := c.info()
				if _, err := c.zk.Create(path.Join(c.workersPath, c.config.NodeId), info, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
					panic(err)
				}
				return
			}
			c.zk.Delete(c.lockPath, -1)
			panic("partitions already reached " + strconv.Itoa(c.workers.Len()) + "/" + strconv.Itoa(c.config.InitialWorkers))
		}
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) createBarrier(name string, onChange func(*donut.SafeMap)) {
	bPath := path.Join(c.barriersPath, name)
	if _, ok := c.watchers[bPath]; !ok {
		if _, err := c.zk.Create(bPath, "", 0, gozk.WorldACL(gozk.PERM_ALL)); err == nil {
			log.Printf("created barrier %s", bPath)
		} else {
			log.Printf("failed to create barrier %s: %v", bPath, err)
		}
		kill, err := watchZKChildren(c.zk, bPath, donut.NewSafeMap(make(map[string]interface{})), onChange)
		if err != nil {
			panic(err)
		}
		c.watchers[name] = kill
	}
}

func (c *Coordinator) enterBarrier(name, entry, data string) {
	log.Printf("entering barrier %s", name)
	if _, err := c.zk.Create(path.Join(c.barriersPath, name, entry), data, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
		log.Printf("error on barrier entry (%s entering %s): %v", entry, name, err)
		panic(err)
	}
}

func (c *Coordinator) start(zk *gozk.ZooKeeper) {
	c.zk = zk
	c.setup()
	c.register()
}

func (c *Coordinator) startWork(workId string, data map[string]interface{}) {
	if t := data["work"].(string); t == "load" {
		p := data["path"].(string)
		c.graph.Load(p)
		c.enterBarrier("load", p, "")
	} else if t == "superstep" {
		step := int(data["step"].(float64))

		c.createBarrier("superstep-"+strconv.Itoa(step), func(m *donut.SafeMap) {
			c.onStepBarrierChange(step, m)
		})

		log.Printf("ready to run superstep %d", step)
		active, msgs, aggr := c.graph.runSuperstep(step)
		stepData := make(map[string]interface{})
		stepData["active"] = active
		stepData["msgs"] = msgs
		stepData["aggr"] = aggr

		var data []byte
		var err error
		if data, err = json.Marshal(stepData); err != nil {
			panic(err)
		}
		log.Printf("preparing to enter barrier for superstep %d", step)
		c.enterBarrier("superstep-"+strconv.Itoa(step), c.config.NodeId, string(data))
	} else if t == "write" {
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
			log.Printf("len == partitions")
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
			c.workerInfos = make(map[string]map[string]interface{})
			c.rpcClients = make(map[string]*rpc.Client)
			for _, w := range workers {
				// pull down worker info for all of the existing workers
				c.workerInfos[w] = c.workerInfo(w)
				c.rpcClients[w], _ = rpc.DialHTTP("tcp", net.JoinHostPort(c.workerInfos[w]["host"].(string), c.workerInfos[w]["port"].(string)))
			}

			// go into loadstate
			if !atomic.CompareAndSwapInt32(&c.state, SetupState, LoadState) {
				panic("state swap from setup to loadstate failed")
			}
			go c.createLoadWork()
		}
	}
}

func (c *Coordinator) info() string {
	m := make(map[string]interface{})
	m["host"] = c.host
	m["port"] = c.port

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
		atomic.StoreInt32(&c.state, RunState)
		go c.createStepWork(1)
	} else {
		log.Printf("%d != %d", m.Len(), len(c.graph.job.LoadPaths()))
	}
}

func (c *Coordinator) onWriteBarrierChange(m *donut.SafeMap) {
	if m.Len() == c.workers.Len() {
		log.Println("Writes are done, killing job")
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
