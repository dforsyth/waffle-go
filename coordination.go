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

	clients map[string]*rpc.Client
}

func (c *coordinator) setup() {
	c.basePath = path.Join("/", c.config.JobId)
	c.lockPath = path.Join(c.basePath, "lock")
	c.workersPath = path.Join(c.basePath, "workers")
	c.barriersPath = path.Join(c.basePath, "barriers")

	c.zk.Create(c.basePath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	c.zk.Create(c.workersPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))

	c.watchers = make(map[string]chan byte)
	c.partitions = make(map[int]string)

	// start servers
	c.startServer()

	watchZKChildren(c.zk, c.workersPath, c.workers, func(m *donut.SafeMap) {
		c.onWorkersChange(m)
	})
}

func (c *coordinator) startServer() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":5000")
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
	if _, _, err := c.zk.Get(path.Join(c.basePath, "joinable")); err != nil {
		panic("not joinable")
	}
	for {
		if _, err := c.zk.Create(c.lockPath, "", gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
			defer c.zk.Delete(c.lockPath, -1)
			if c.workers.Len() < c.config.Partitions {
				if _, err := c.zk.Create(path.Join(c.workersPath, c.config.NodeId), "", 0, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
					panic(err)
				}
				return
			}
			panic("partitions already reached")
		}
		time.Sleep(time.Second)
	}
}

func (c *coordinator) createBarrier(name string, onChange func(*donut.SafeMap)) {
	bPath := path.Join(c.barriersPath, name)
	c.zk.Create(bPath, "", 0, gozk.WorldACL(gozk.PERM_ALL))
	kill, err := watchZKChildren(c.zk, bPath, donut.NewSafeMap(make(map[string]interface{})), onChange)
	if err != nil {
		panic(err)
	}
	c.watchers[name] = kill
}

func (c *coordinator) enterBarrier(name, entry, data string) {
	if _, err := c.zk.Create(path.Join(c.barriersPath, name, entry), data, gozk.EPHEMERAL, gozk.WorldACL(gozk.PERM_ALL)); err != nil {
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
		c.createBarrier("load", nil)
		c.graph.Load(p)
		c.enterBarrier("load", p, "")
	} else if t == "superstep" {
		step := data["step"].(int)

		c.createBarrier("superstep-"+strconv.Itoa(step), func(m *donut.SafeMap) {
			c.onStepBarrierChange(step, m)
		})

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

		c.enterBarrier("superstep-"+strconv.Itoa(step), c.config.NodeId, string(data))
	} else if t == "write" {
		c.createBarrier("write", nil)
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
		lm := m.RangeLock()
		for k := range lm {
			if data, _, err := c.zk.Get(path.Join(c.barriersPath, barrierName, k)); err != nil {
				var info map[string]interface{}
				if err := json.Unmarshal([]byte(data), &info); err != nil {
					panic(err)
				}
				c.graph.globalStat.active += info["active"].(int)
				c.graph.globalStat.msgs += info["msgs"].(int)
			} else {
				panic(err)
			}
		}
		m.RangeUnlock()
		// kill the watcher on this barrier
		c.watchers[barrierName] <- 1
		delete(c.watchers, barrierName)
		if c.graph.globalStat.active == 0 && c.graph.globalStat.msgs == 0 {
			go c.createWriteWork()
		} else {
			go c.createStepWork(step + 1)
		}
	}
}

func (c *coordinator) onWorkersChange(m *donut.SafeMap) {
	if c.state > SETUP {
		// invalidate current step
		// update partition mapping
		// roll back to last checkpoint
	} else {
		if m.Len() == c.workers.Len() {
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
			for _, w := range workers {
				// pull down worker info for all of the existing workers
				c.workerInfos[w] = c.workerInfo(w)
				c.clients[w], _ = rpc.DialHTTP("tcp", net.JoinHostPort(c.workerInfos[w]["host"].(string), c.workerInfos[w]["port"].(string)))
			}
		}
	}
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
	if m.Len() == c.workers.Len() {
		c.watchers["load"] <- 1
		delete(c.watchers, "load")
		c.createStepWork(1)
	}
}

func (c *coordinator) createWriteWork() {
	panic("not implemented")
}

func (c *coordinator) createStepWork(step int) {
	data := make(map[string]interface{})
	data[c.clusterName] = c.config.NodeId
	data["work"] = "superstep"
	data["step"] = step
	donut.CreateWork(c.clusterName, c.zk, c.donutConfig, "superstep-"+strconv.Itoa(step+1)+"-"+c.config.NodeId, data)
}
