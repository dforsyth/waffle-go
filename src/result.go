package waffle

type ResultWriter interface {
	WriteResults(*Worker) error
}
