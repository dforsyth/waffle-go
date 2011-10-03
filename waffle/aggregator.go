package waffle

import (
	"os"
)

type Aggregator interface {
	Init(*Worker)
	Aggregate() (string, interface{}, os.Error)
	BeforeCompute()
	AfterCompute()
}

type AggregatorBase struct {
	Component
}
