package waffle

// messages passed from vert to vert
type Msg interface {
	DestVertId() string
	SetDestVertId(string)
}

type MsgBase struct {
	DestId string
}

func (m *MsgBase) DestVertId() string {
	return m.DestId
}

func (m *MsgBase) SetDestVertId(dest string) {
	m.DestId = dest
}

const (
	vadd = iota
	vrem
	eadd
	erem
)

type MutationMsg struct {
	MsgBase
	MutType int
	Change  interface{}
}

type RegisterInfo struct {
	Addr string
	Port string
}

type RegisterResp struct {
	JobId    string
	WorkerId string
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
	JobId       string
	WorkerId    string
	PhaseId     int
	Error       error
	ActiveVerts uint64
	NumVerts    uint64
	SentVerts   uint64
	SentMsgs    uint64
	PhaseTime   int64
}
