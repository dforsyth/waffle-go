package waffle

type Persister interface {
	Write(*Worker) error // Persist worker state
	Read(*Worker) error  // Read persisted worker state
}
