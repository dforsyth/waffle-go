package waffle

import (
	"os"
)

type WorkerRpcClient interface {
	Init()
	Register(string, string, string) (string, string, os.Error)
	PhaseResult(string, *PhaseSummary) os.Error
	SendVertices(string, []Vertex) os.Error
	SendMessages(string, []Msg) os.Error
	// Cleanup()
}

type MasterRpcClient interface {
	Init()
	PushTopology(string, *TopologyInfo) os.Error
	ExecutePhase(string, *PhaseExec) os.Error
	// Cleanup()
}

type MasterRpcServer interface {
	Start(*Master)
	// Cleanup()
}

type WorkerRpcServer interface {
	Start(*Worker)
	// Cleanup()
}
