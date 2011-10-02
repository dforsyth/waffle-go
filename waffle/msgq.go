package waffle

import (
	"os"
	"sync"
)

type MsgQ interface {
	addMsg(Msg)
}

type InMsgQ struct {
	s  chan byte
	in map[string][]Msg
}

func newInMsgQ() *InMsgQ {
	q := &InMsgQ{
		s:  make(chan byte, 1),
		in: make(map[string][]Msg),
	}
	q.s <- 1
	return q
}

func (q *InMsgQ) addMsg(msg Msg) {
	<-q.s
	defer func() { q.s <- 1 }()
	if _, ok := q.in[msg.DestVertId()]; !ok {
		q.in[msg.DestVertId()] = make([]Msg, 0)
	}
	q.in[msg.DestVertId()] = append(q.in[msg.DestVertId()], msg)
}

func (q *InMsgQ) addMsgs(msgs []Msg) {
	<-q.s
	defer func() { q.s <- 1 }()
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
	s      chan byte
	out    map[string][]Msg
	w      *Worker
	thresh int64
	sent   uint64
	wait   sync.WaitGroup
}

func newOutMsgQ(w *Worker, thresh int64) *OutMsgQ {
	q := &OutMsgQ{
		s:      make(chan byte, 1),
		out:    make(map[string][]Msg),
		w:      w,
		thresh: thresh,
		sent:   0,
	}
	q.s <- 1
	return q
}

func (q *OutMsgQ) numSent() uint64 {
	return q.sent
}

func (q *OutMsgQ) reset() {
	q.out = make(map[string][]Msg)
	q.sent = 0
}

func (q *OutMsgQ) sendMsgs(wid string, msgs []Msg) os.Error {
	cl, e := q.w.cl(wid)
	if e != nil {
		return e
	}
	if wid == q.w.wid {
		q.w.inq.addMsgs(msgs)
	} else {
		var r Resp
		if e = cl.Call("Worker.MsgHandler", msgs, &r); e != nil {
			return e
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
	<-q.s
	defer func() { q.s <- 1 }()
	pid := q.w.getPartitionOf(msg.DestVertId())
	wid := q.w.pmap[pid]
	if _, ok := q.out[wid]; !ok {
		q.out[wid] = make([]Msg, 0)
	}
	q.out[wid] = append(q.out[wid], msg)
	if int64(len(q.out[wid])) >= q.thresh {
		msgs := q.out[wid]
		q.out[wid] = nil, false
		q.sendMsgsAsync(wid, msgs)
	}
}

func (q *OutMsgQ) flush() {
	<-q.s
	defer func() { q.s <- 1 }()
	for wid, msgs := range q.out {
		if e := q.sendMsgs(wid, msgs); e != nil {
			panic(e.String())
		}
	}
	for wid := range q.out {
		q.out[wid] = nil, false
	}
}
