package waffle

import (
	"donut"
)

type waffleBalancer struct {
	l *waffleListener
}

func (b *waffleBalancer) Init(l donut.Listener) {
	b.l = l.(*waffleListener)
}

func (b *waffleBalancer) AddWork(id string) {}

func (b *waffleBalancer) RemoveWork(string) {}

func (b *waffleBalancer) CanClaim() bool {
	return true
}

func (b *waffleBalancer) HandoffList() []string {
	return make([]string, 0)
}
