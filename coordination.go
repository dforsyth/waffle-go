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
	"strconv"
	"time"
)

const (
	SETUP = iota
	LOAD
	RUN
	WRITE
)

type Config struct {
	NodeId     string
	JobId      string
	Partitions int
}

type coordinator struct {
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

	clients    map[string]*rpc.Client
	host, port string
}

func (c *coordinator) setup() {
	c.basePath = path.Join("/", c.config.JobId)
	c.lockPath = path.Join(c.basePath, "lock")
	c.workersPath = path.Join(c.basePath, "workers")
	c.barriersPath = path.Join(c.basePath, "barriers")

	c.zk.Create(c.basePath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(c.workersPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(c.barriersPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))

	c.watchers = make(map[string]chan byte)
	c.partitions = make(map[int]string)
	c.workers = donut.NewSafeMap(nil)

	// start servers
	c.startServer()

	watchZKChildren(c.zk, c.workersPath, c.workers, func(m *donut.SafeMap) {
		c.onWorkersChange(m)
	})
}

func (c *coordinator) startServer() {
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

func (c *coordinator) SubmitVertex(v Vertex, r *int) error {
	c.graph.addVertex(v)
	*r = 0
	return nil
}

func (c *coordinator) sendVertex(v Vertex, pid int) error {
	w := c.partitions[pid]
	cl := c.clients[w]
	var r int
	return cl.Call("coordinator.SubmitVertex", v, &r)
}

func (c *coordinator) SubmitEdge(e Edge, r *int) error {
	c.graph.addEdge(e)
	*r = 0
	return nil
}

func (c *coordinator) sendEdge(e Edge, pid int) error {
	w := c.partitions[pid]
	cl := c.clients[w]
	var r int
	return cl.Call("coordinator.SubmitEdge", e, &r)
}

func (c *coordinator) SubmitMessage(m Message, r *int) error {
	c.graph.addMessage(m)
	*r = 0
	return nil
}

func (c *coordinator) sendMessage(m Message, pid int) error {
	w := c.partitions[pid]
	cl := c.clients[w]
	var r int
	return cl.Call("coordinator.SubmitMessage", m, &r)
}

func (c *coordinator) register() {
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
			if c.workers.Len() < c.config.Partitions {
				info := c.info()
				if _, err := c.zk.Create(path.Join(c.workersPath, c.config.NodeId), info, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
					panic(err)
				}
				return
			}
			c.zk.Delete(c.lockPath, -1)
			panic("partitions already reached " + strconv.Itoa(c.workers.Len()) + "/" + strconv.Itoa(c.config.Partitions))
		}
		time.Sleep(time.Second)
	}
}

func (c *coordinator) createBarrier(name string, onChange func(*donut.SafeMap)) {
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

func (c *coordinator) enterBarrier(name, entry, data string) {
	log.Printf("entering barrier %s", name)
	if _, err := c.zk.Create(path.Join(c.barriersPath, name, entry), data, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
		log.Printf("error on barrier entry (%s): %v", name, err)
		panic(err)
	}
}

func (c *coordinator) start(zk *gozk.ZooKeeper) {
	c.zk = zk
	c.setup()
	c.register()
}

func (c *coordinator) startWork(workId string, data map[string]interface{}) {
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

func (c *coordinator) onStepBarrierChange(step int, m *donut.SafeMap) {
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
			go c.createWriteWork()
		} else {
			go c.createStepWork(step + 1)
		}
	} else {
		log.Printf("step barrier change: %d != %d", m.Len(), c.workers.Len())
	}
}

func (c *coordinator) onWorkersChange(m *donut.SafeMap) {
	log.Println("updated")
	if c.state > SETUP {
		// invalidate current step
		// update partition mapping
		// roll back to last checkpoint
	} else {
		if m.Len() == c.config.Partitions {
			log.Printf("len == partitions")
			// everyone is here, create the partition mapping
			lm := m.RangeLock()
			var workers []string
			for k := range lm {
				workers = append(workers, k)
			}
			m.RangeUnlock()
			for i := 0; i < len(workers); i++ {
				c.partitions[i] = workers[i]
			}

			// set up connections to all the other nodes
			c.workerInfos = make(map[string]map[string]interface{})
			c.clients = make(map[string]*rpc.Client)
			for _, w := range workers {
				// pull down worker info for all of the existing workers
				c.workerInfos[w] = c.workerInfo(w)
				c.clients[w], _ = rpc.DialHTTP("tcp", net.JoinHostPort(c.workerInfos[w]["host"].(string), c.workerInfos[w]["port"].(string)))
			}
			go c.createLoadWork()
		}
	}
}

func (c *coordinator) info() string {
	m := make(map[string]interface{})
	m["host"] = c.host
	m["port"] = c.port

	info, _ := json.Marshal(m)
	return string(info)
}

func (c *coordinator) workerInfo(id string) (info map[string]interface{}) {
	raw, _, err := c.zk.Get(path.Join(c.workersPath, id))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		panic(err)
	}
	return info
}

func (c *coordinator) onLoadBarrierChange(m *donut.SafeMap) {
	if m.Len() == len(c.graph.job.LoadPaths()) {
		log.Printf("load complete")
		c.watchers["load"] <- 1
		delete(c.watchers, "load")
		go c.createStepWork(1)
	} else {
		log.Printf("%d != %d", m.Len(), len(c.graph.job.LoadPaths()))
	}
}

func (c *coordinator) onWriteBarrierChange(m *donut.SafeMap) {
	if m.Len() == c.workers.Len() {
		log.Printf("done write, killing job")
		panic("done")
	}
}

func (c *coordinator) createWriteWork() {
	log.Printf("creating work for write %s", c.config.NodeId)
	data := make(map[string]interface{})
	data[c.clusterName] = c.config.NodeId
	data["work"] = "write"
	donut.CreateWork(c.clusterName, c.zk, c.donutConfig, "write-"+c.config.NodeId, data)
}

func (c *coordinator) createLoadWork() {
	log.Println("creating load work")
	data := make(map[string]interface{})
	// data[c.clusterName] = c.config.NodeId
	data["work"] = "load"
	paths := c.graph.job.LoadPaths()
	// create the load barrier here since a node might not end up with load work
	c.createBarrier("load", func(m *donut.SafeMap) {
		c.onLoadBarrierChange(m)
	})
	for _, p := range paths {
		data["path"] = p
		if err := donut.CreateWork(c.clusterName, c.zk, c.donutConfig, "load-"+p, data); err != nil {
			log.Printf("create work failed: %s", err)
		}
	}
}

func (c *coordinator) createStepWork(step int) {
	log.Printf("creating work for superstep %d", step)
	data := make(map[string]interface{})
	data[c.clusterName] = c.config.NodeId
	data["work"] = "superstep"
	data["step"] = step
	donut.CreateWork(c.clusterName, c.zk, c.donutConfig, "superstep-"+strconv.Itoa(step)+"-"+c.config.NodeId, data)
}
