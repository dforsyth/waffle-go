package waffle

import (
	"os"
)

type Persister interface {
	Write(*Worker) os.Error // Persist worker state
	Read(*Worker) os.Error  // Read persisted worker state
}
