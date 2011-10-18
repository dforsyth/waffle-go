package waffle

import (
	"os"
)

type Loader interface {
	Load(w *Worker) (uint64, os.Error)
}

type LoaderBase struct {

}

func (*LoaderBase) AddVertex(w *Worker, v Vertex) {
	w.addToPartition(v)
}
