package waffle

import (
	"os"
)

type Persister interface {
	Init(*Worker)
	Write() os.Error // Persist worker state
	Read() os.Error  // Read persisted worker state
}

type PersisterBase struct {
	Component
}
