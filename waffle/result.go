package waffle

import (
	"os"
)

type ResultWriter interface {
	WriteResults(*Worker) os.Error
}
