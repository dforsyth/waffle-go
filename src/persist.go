package waffle

type Persister interface {
	Persist(uint64, uint64, []Vertex, []Msg) error
	Load(uint64, uint64) ([]Vertex, []Msg, error)
}
