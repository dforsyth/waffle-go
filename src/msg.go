package waffle

// messages passed from vert to vert
type Msg interface {
	DestVertId() string
	SetDestVertId(string)
}

type Resp int

const (
	OK = iota
	NOT_OK
)

type MsgBase struct {
	DestId string
}

func (m *MsgBase) DestVertId() string {
	return m.DestId
}

func (m *MsgBase) SetDestVertId(dest string) {
	m.DestId = dest
}

type RemoveVertexMsg struct {
	MsgBase
}

type RemoveEdgeMsg struct {
	MsgBase
	TargetId string
}

type AddVertexMsg struct {
	MsgBase
	V Vertex
}

type AddEdgeMsg struct {
	MsgBase
	E Edge
}

type BasicMasterMsg struct {
	JobId string
}

type RegisterInfo struct {
	Addr string
	Port string
}

type RegisterResp struct {
	WorkerId string
	JobId    string
}

type TopologyInfo struct {
	JobId        string
	WorkerMap    map[string]string
	PartitionMap map[uint64]string
}

type PhaseExec struct {
	JobId       string
	PhaseId     int
	Superstep   uint64
	Checkpoint  bool
	NumVerts    uint64
	ActiveVerts uint64
}

type PhaseSummary struct {
	JobId    string
	WorkerId string
	PhaseId  int
	// Errors []os.Error
	ActiveVerts uint64
	NumVerts    uint64
	SentVerts   uint64
	SentMsgs    uint64
	PhaseTime   int64
}
