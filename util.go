package waffle

import (
	"donut"
	"gozk"
	"log"
)

// XXX pulled this out of donut, maybe i should make a zk util lib?
// Watch the children at path until a byte is sent on the returned channel
// Uses the SafeMap more like a set, so you'll have to use Contains() for entries
func watchZKChildren(zk *gozk.ZooKeeper, path string, children *donut.SafeMap, onChange func(*donut.SafeMap)) (chan byte, error) {
	initial, _, watch, err := zk.ChildrenW(path)
	if err != nil {
		return nil, err
	}
	m := children.RangeLock()
	for _, node := range initial {
		m[node] = nil
	}
	children.RangeUnlock()
	kill := make(chan byte)
	go func() {
		defer close(kill)
		var nodes []string
		var err error
		for {
			select {
			case <-kill:
				// close(watch)
				return
			case event := <-watch:
				if !event.Ok() {
					continue
				}
				// close(watch)
				nodes, _, watch, err = zk.ChildrenW(path)
				if err != nil {
					log.Printf("Error in watchZkChildren: %v", err)
					// XXX I should really provide some way for the client to find out about this error...
					return
				}
				m := children.RangeLock()
				// mark all dead
				for k := range m {
					m[k] = 0
				}
				for _, node := range nodes {
					m[node] = 1
				}
				for k, v := range m {
					if v.(int) == 0 {
						delete(m, k)
					}
				}
				children.RangeUnlock()
				onChange(children)
			}
		}
	}()
	log.Printf("watcher setup on %s", path)
	return kill, nil
}
