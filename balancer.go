package waffle

import (
	"donut"
)

type waffleBalancer struct {
}

func (b *waffleBalancer) Init(l *donut.Cluster) {
}

func (b *waffleBalancer) CanClaim() bool {
	return true
}

func (b *waffleBalancer) HandoffList() []string {
	return make([]string, 0)
}
