package waffle

type Loader interface {
	Load(w *Worker) (uint64, error)
}
