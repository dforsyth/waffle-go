package waffle

type Aggregator interface {
	Aggregate(*Worker) (string, interface{}, error)
	BeforeCompute(*Worker)
	AfterCompute(*Worker)
}
