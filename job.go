package waffle

type Job interface {
	Id() string
	LoadPaths() []string
	Load(string) ([]Vertex, []Edge, error)
	Checkpoint(int) bool
	Write() error
	Persist() error
}
