package waffle

import (
	"os"
)

type Loader interface {
	Init(*Worker)
	Load() (int, os.Error)
}

type LoaderBase struct {
	Component
}

func (l *LoaderBase) AddVertex(v Vertex) {
	l.w.addToPartition(v)
}
