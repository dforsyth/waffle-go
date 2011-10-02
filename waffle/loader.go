package waffle

import (
	"os"
)

type Loader interface {
	Load(w *Worker) (int, os.Error)
}
