package waffle

type Persister interface {
	Write(uint64, []Vertex, []Msg) error
	Read(uint64) ([]Vertex, []Msg, error)
}
