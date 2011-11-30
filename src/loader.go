package waffle

type Loader interface {
	Load(w *Worker, path string) (uint64, error)
	AssignLoad([]string, []string) map[string][]string
}
