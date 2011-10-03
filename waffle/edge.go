package waffle

import ()

type Edge interface {
	Target() string
	SetTarget(string)
}

type EdgeBase struct {
	TargetId string
}

func (e *EdgeBase) Target() string {
	return e.TargetId
}

func (e *EdgeBase) SetTarget(id string) {
	e.TargetId = id
}
