package waffle

import (
	"donut"
)

type Config struct {
	NodeId           string
	JobId            string
	InitialWorkers   int
	RPCHost, RPCPort string
	ZKServers        string
}

func Run(c *Config, j Job) {
	clusterName := j.Id()
	listener := &waffleListener{
		clusterName: clusterName,
		coordinator: newCoordinator(clusterName, c),
		job:         j,
	}
	balancer := &waffleBalancer{}
	config := donut.NewConfig()
	config.Servers = c.ZKServers
	config.NodeId = c.NodeId
	config.Timeout = 1 * 1e9

	cluster := donut.NewCluster(clusterName, config, balancer, listener)

	listener.cluster = cluster
	listener.done = make(chan byte)
	listener.config = config
	listener.coordinator.done = listener.done
	cluster.Join()
	<-listener.done
}

const (
	BarriersPath = "barriers"
	LockPath     = "lock"
	WorkersPath  = "workers"
)
