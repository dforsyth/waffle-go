package waffle

import "sync"

type MsgQ interface {
	addMsg(Msg)
}

type InMsgQ struct {
	m  sync.Mutex
	in map[string][]Msg
}

func newInMsgQ() *InMsgQ {
	q := &InMsgQ{
		in: make(map[string][]Msg),
	}
	return q
}

func (q *InMsgQ) addMsg(msg Msg) {
	q.m.Lock()
	defer q.m.Unlock()
	if _, ok := q.in[msg.DestVertId()]; !ok {
		q.in[msg.DestVertId()] = make([]Msg, 0)
	}
	q.in[msg.DestVertId()] = append(q.in[msg.DestVertId()], msg)
}

func (q *InMsgQ) addMsgs(msgs []Msg) {
	q.m.Lock()
	defer q.m.Unlock()
	for _, msg := range msgs {
		if _, ok := q.in[msg.DestVertId()]; !ok {
			q.in[msg.DestVertId()] = make([]Msg, 0)
		}
		q.in[msg.DestVertId()] = append(q.in[msg.DestVertId()], msg)
	}
}

func (q *InMsgQ) msgs(vid string) []Msg {
	return q.in[vid]
}

func (q *InMsgQ) clear() {
	q.in = make(map[string][]Msg)
}

type OutMsgQ struct {
	m      sync.Mutex
	out    map[string][]Msg
	worker *Worker
	thresh int64
	sent   uint64
	wait   sync.WaitGroup
}

func newOutMsgQ(w *Worker, thresh int64) *OutMsgQ {
	q := &OutMsgQ{
		out:    make(map[string][]Msg),
		worker: w,
		thresh: thresh,
		sent:   0,
	}
	return q
}

func (q *OutMsgQ) numSent() uint64 {
	return q.sent
}

func (q *OutMsgQ) reset() {
	q.out = make(map[string][]Msg)
	q.sent = 0
}

func (q *OutMsgQ) sendMsgs(wid string, msgs []Msg) error {
	for _, combiner := range q.worker.combiners {
		msgs = combiner.Combine(msgs)
	}

	if q.worker.WorkerId() == wid {
		q.worker.inq.addMsgs(msgs)
	} else {
		if err := q.worker.rpcClient.SendMessages(wid, msgs); err != nil {
			return err
		}
	}

	q.sent += uint64(len(msgs))
	return nil
}

func (q *OutMsgQ) sendMsgsAsync(id string, msgs []Msg) chan interface{} {
	ch := make(chan interface{})
	q.wait.Add(1)
	go func() {
		if e := q.sendMsgs(id, msgs); e != nil {
			ch <- e
		}
		q.wait.Done()
	}()
	return ch
}

func (q *OutMsgQ) addMsg(msg Msg) {
	q.m.Lock()
	defer q.m.Unlock()
	pid := q.worker.partitionOf(msg.DestVertId())
	wid := q.worker.partitionMap[pid]
	if _, ok := q.out[wid]; !ok {
		q.out[wid] = make([]Msg, 0)
	}
	q.out[wid] = append(q.out[wid], msg)
	if int64(len(q.out[wid])) >= q.thresh {
		msgs := q.out[wid]
		delete(q.out, wid)
		q.sendMsgsAsync(wid, msgs)
	}
}

func (q *OutMsgQ) flush() {
	q.m.Lock()
	defer q.m.Unlock()
	for wid, msgs := range q.out {
		if e := q.sendMsgs(wid, msgs); e != nil {
			panic(e.Error())
		}
	}
	for wid := range q.out {
		delete(q.out, wid)
	}
}
