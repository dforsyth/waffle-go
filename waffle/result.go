package waffle

import (
	"os"
)

type ResultWriter interface {
	Init(*Worker)
	WriteResults() os.Error
}

type ResultWriterBase struct {
	Component
}
