package waffle

import (
	"os"
)

type Loader interface {
	Load(w *Worker) (uint64, os.Error)
}
