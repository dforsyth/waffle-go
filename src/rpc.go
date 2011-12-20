package waffle

type WorkerRpcClient interface {
	// Init()
	Register(string, string, string) (string, error)
	SendSummary(string, PhaseSummary) error
	SendVertices(string, []Vertex) error
	SendMessages(string, []Msg) error
	// Cleanup()
}

type MasterRpcClient interface {
	// Init()
	PushTopology(string, *TopologyInfo) error
	ExecutePhase(string, PhaseExec) error
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
