package waffle

import (
	"donut"
	"launchpad.net/gozk/zookeeper"
	"log"
)

type waffleListener struct {
	coordinator *Coordinator

	clusterName string

	done    chan byte
	zk      *zookeeper.Conn
	job     Job
	config  *donut.Config
	cluster *donut.Cluster
}

func (l *waffleListener) OnJoin(zk *zookeeper.Conn) {
	log.Println("waffle onjoin")
	l.zk = zk
	l.coordinator.graph = newGraph(l.job, l.coordinator)
	l.coordinator.donutConfig = l.config
	if err := l.coordinator.start(zk); err != nil {
		l.cluster.Shutdown()
	}
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

func (l *waffleListener) Information() map[string]interface{} {
	return l.coordinator.graph.information()
}
