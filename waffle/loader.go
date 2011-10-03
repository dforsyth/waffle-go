package waffle

import (
	"os"
)

type Loader interface {
	Init(w *Worker)
	Load() (int, os.Error)
}

type LoaderBase struct {
	w *Worker
}

func (l *LoaderBase) Init(w *Worker) {
	l.w = w
}

func (l *LoaderBase) AddVertex(v Vertex) {
	l.w.addToPartition(v)
}
