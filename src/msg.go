package waffle

// messages passed from vert to vert
type Msg interface {
	Target() string
	SetTarget(string)
}

type MsgBase struct {
	TargetId string
}

func (m *MsgBase) Target() string {
	return m.TargetId
}

func (m *MsgBase) SetTarget(target string) {
	m.TargetId = target
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
	Addr          string
	Port          string
	MinPartitions uint64
}

type RegisterResp struct {
	JobId string
}

type TopologyInfo struct {
	JobId        string
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
	Errors      []error
	ActiveVerts uint64
	NumVerts    uint64
	SentVerts   uint64
	SentMsgs    uint64
	PhaseTime   int64
	PhaseInfo   map[string]interface{}
}

func (ps *PhaseSummary) worker() string {
	return ps.WorkerId
}
