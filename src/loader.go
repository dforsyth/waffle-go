package waffle

type Loader interface {
	Load(w *Worker, path string) (uint64, error)
}
