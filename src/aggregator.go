package waffle

import (
	"os"
)

type Aggregator interface {
	Aggregate(*Worker) (string, interface{}, os.Error)
	BeforeCompute(*Worker)
	AfterCompute(*Worker)
}
