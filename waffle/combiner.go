package waffle

type Combiner interface {
	Combine([]Msg) []Msg
}
