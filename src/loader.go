package waffle

import (
	"os"
)

type Loader interface {
	Init(*Worker)
	Load() (uint64, os.Error)
}

type LoaderBase struct {
	w *Worker
}

func (l *LoaderBase) Init(w *Worker) {
	l.w = w
}

func (l *LoaderBase) AddVertex(v Vertex) {
	l.w.addVertex(v)
}
