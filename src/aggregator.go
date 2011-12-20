package waffle

type Aggregator interface {
	Name() string
	Reset()
	Submit(interface{})
	ReduceAndEmit() interface{}
}

type SumAggregator struct {
	values []int
}

func (a *SumAggregator) Name() string {
	return "sum"
}

func (a *SumAggregator) Reset() {
	if a.values != nil {
		a.values = a.values[0:0]
	}
}

func (a *SumAggregator) Submit(v interface{}) {
	if val, ok := v.(int); ok {
		a.values = append(a.values, val)
	} else {
		panic("non-int submitted to sum aggregator")
	}
}

func (a *SumAggregator) ReduceAndEmit() interface{} {
	sum := 0
	for _, val := range a.values {
		sum += val
	}
	return sum
}
