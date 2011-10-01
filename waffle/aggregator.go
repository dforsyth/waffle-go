package waffle

import (
	"os"
)

type Aggregator interface {
	Aggregate(*Worker) (string, string, os.Error)
	BeforeCompute()
	AfterCompute()
}
