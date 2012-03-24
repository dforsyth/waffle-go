package waffle

import (
	"donut"
	"gozk"
)

type waffleListener struct {
	coordinator *coordinator

	clusterName string

	graph  *Graph
	done   chan byte
	zk     *gozk.ZooKeeper
	job    Job
	config *donut.Config
}

func (l *waffleListener) OnJoin(zk *gozk.ZooKeeper) {
	l.zk = zk

	l.graph = &Graph{}

	l.coordinator.start(zk)
}

func (l *waffleListener) StartWork(workId string, data map[string]interface{}) {
	l.coordinator.startWork(workId, data)
}

func (l *waffleListener) EndWork(workId string) {

}

func (l *waffleListener) OnLeave() {
	defer func() {
		l.done <- 1
	}()
}
