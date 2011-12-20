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

type PhaseExec interface {
	JobId() string
	PhaseId() int
}

type ExecBase struct {
	JId string
	PId int
}

func (e *ExecBase) JobId() string {
	return e.JId
}

func (e *ExecBase) PhaseId() int {
	return e.PId
}

type LoadDataExec struct {
	ExecBase
	LoadAssignments map[string][]string
}

type LoadRecievedExec struct {
	ExecBase
}

type StepPrepareExec struct {
	ExecBase
}

type SuperstepExec struct {
	ExecBase
	Superstep   uint64
	Checkpoint  bool
	TotalVerts  uint64
	ActiveVerts uint64
	Aggregates  map[string]interface{}
}

type WriteResultsExec struct {
	ExecBase
}

type PhaseSummary interface {
	JobId() string
	WorkerId() string
	PhaseId() int
}

type SummaryBase struct {
	JId string
	WId string
	PId int
}

func (s *SummaryBase) JobId() string {
	return s.JId
}

func (s *SummaryBase) WorkerId() string {
	return s.WId
}

func (s *SummaryBase) PhaseId() int {
	return s.PId
}

type LoadDataSummary struct {
	SummaryBase
	LoadedVerts uint64
	Error       string
}

type LoadRecievedSummary struct {
	SummaryBase
	TotalVerts  uint64
	ActiveVerts uint64 // needed?
}

type StepPrepareSummary struct {
	SummaryBase
}

type SuperstepSummary struct {
	SummaryBase
	ActiveVerts uint64
	SentMsgs    uint64
	TotalVerts  uint64
	Aggregates  map[string]interface{}
	Error       string
}

type WriteResultsSummary struct {
	SummaryBase
	Error string
}
