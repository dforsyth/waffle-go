package waffle

import (
	"donut"
	"time"
)

const (
	vPort = 5000
	ePort = 6000
	mPort = 7000
)

type Persister interface {
	Persist()
}

type Writer interface {
	Write()
}

func Run(c *Config, j Job) {
	clusterName := j.Id() + "-" + time.Now().String()
	listener := &waffleListener{
		clusterName: clusterName,
		coordinator: &coordinator{
			config: c,
		},
	}
	balancer := &waffleBalancer{}
	config := donut.NewConfig()

	cluster := donut.NewCluster(clusterName, config, balancer, listener)

	listener.done = make(chan byte)
	cluster.Join()
	<-listener.done
}

const (
	barriers = "barriers"
	joinable = "joinable"
	ready    = "ready"
	lock     = "lock"
	workers  = "workers"
	// coordinator = "coordinator"
)
