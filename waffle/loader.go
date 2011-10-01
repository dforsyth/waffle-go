package waffle

import (
	"os"
)

type Loader interface {
	Load(w *Worker) ([]Vertex, os.Error)
}

type BasicLoader struct {
	w *Worker
}

func (l *BasicLoader) Worker() *Worker {
	return l.w
}
