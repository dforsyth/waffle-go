package waffle

import (
	"batter"
)

type Msg interface {
	batter.Msg
	VertexId() string
	SetVertexId(string)
}

type MsgBase struct {
	batter.MsgBase
	Vert string
}

func (m *MsgBase) VertexId() string {
	return m.Vert
}

func (m *MsgBase) SetVertexId(id string) {
	m.Vert = id
}

type VertexMsg struct {
	batter.MsgBase
	Vertices []Vertex
}

const (
	vadd = iota
	vrem
	eadd
	erem
)

type MutationMsg struct {
	batter.MsgBase
	MutType int
	Change  interface{}
}
