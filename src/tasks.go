package waffle

import (
	"batter"
	"log"
	"runtime"
	"sync"
)

type WaffleTask interface {
	SetWorker(*Worker)
}

type waffleTaskBase struct {
	w *Worker
}

func (t *waffleTaskBase) SetWorker(w *Worker) {
	t.w = w
}

func (t *waffleTaskBase) TaskName() string {
	return "waffle"
}

type LoadTask struct {
	batter.TaskerBase
	PartitionMap map[uint64]string
	Assignments  map[string][]string // maybe just bring this down to a []string.  does every worker really need to know what the other loaded?
	waffleTaskBase
}

type LoadTaskResponse struct {
	batter.TaskerBase
	Errors      []error
	TotalLoaded uint64
}

func (t *LoadTask) Execute() (batter.TaskResponse, error) {
	// set the partition map
	t.w.partitionMap = t.PartitionMap
	for pid, hp := range t.w.partitionMap {
		if hp == t.w.WorkerId() {
			t.w.partitions[pid] = NewPartition(pid, t.w)
		}
	}

	var assigned []string
	var ok bool
	// w is set on the way in
	if assigned, ok = t.Assignments[t.w.WorkerId()]; !ok {
		log.Printf("no load assignments for %s", t.w.WorkerId())
		return &LoadTaskResponse{}, nil
	}

	t.w.voutq.Start()

	var totalLoaded uint64
	for _, assignment := range assigned {
		loaded, err := t.w.loader.Load(t.w, assignment)
		if err != nil {
			return &LoadTaskResponse{Errors: []error{err}}, nil
		}
		totalLoaded += loaded
	}

	log.Printf("total loaded: %d", totalLoaded)

	// flush vertices and voutq
	t.w.FlushVertices()
	errors := t.w.voutq.Finish()

	for _, error := range errors {
		log.Printf("voutq error: %v", error)
	}

	return &LoadTaskResponse{TotalLoaded: totalLoaded, Errors: errors}, nil
}

type DistributeTask struct {
	batter.TaskerBase
	waffleTaskBase
}

type DistributeTaskResponse struct {
	batter.TaskerBase
	Errors []error
}

func (t *DistributeTask) Execute() (batter.TaskResponse, error) {
	t.w.vinq.Drain()
	for msg := range t.w.vinq.Spout {
		for _, v := range msg.(*VertexMsg).Vertices {
			t.w.AddVertex(v)
		}
	}
	t.w.vinq.Ready()

	return &DistributeTaskResponse{}, nil
}

type PrepareTask struct {
	batter.TaskerBase
	waffleTaskBase
	Superstep uint64
}

type PrepareTaskResponse struct {
	batter.TaskerBase
}

func (t *PrepareTask) Execute() (batter.TaskResponse, error) {
	if t.Superstep > 0 {
		t.w.minq.Drain()
		t.w.msgs, t.w.nmsgs = t.w.nmsgs, make(map[string][]Msg)
		runtime.GC() // clean up old msgs?
	}
	t.w.minq.Ready()
	go t.w.RecieveMsgs(t.w.minq)
	return &PrepareTaskResponse{}, nil
}

type SuperstepTask struct {
	batter.TaskerBase
	waffleTaskBase
	Superstep  uint64
	Checkpoint bool
	Aggrs      map[string]interface{}
}

type SuperstepTaskResponse struct {
	batter.TaskerBase
	Errors []error
	Info   *stepInfo
	Aggrs  map[string]interface{}
}

func (t *SuperstepTask) Execute() (batter.TaskResponse, error) {
	if t.Checkpoint {
		if err := t.w.persistPartitions(t.Superstep); err != nil {
			return &SuperstepTaskResponse{Errors: []error{err}}, nil
		}
	}

	for _, aggr := range t.w.aggregators {
		aggr.Reset()
	}

	var pWait sync.WaitGroup
	collectCh := make(chan *stepInfo, len(t.w.partitions))
	t.w.moutq.Start()
	for _, p := range t.w.partitions {
		pp := p
		pWait.Add(1)
		go func() {
			pp.compute(t.Superstep, t.Aggrs, collectCh)
			pWait.Done()
		}()
	}
	pWait.Wait()

	collected := newStepInfo()
	for i := 0; i < len(t.w.partitions); i++ {
		info := <-collectCh
		collected = collectStepData(collected, info)
	}

	// drain and restart moutq
	errors := t.w.moutq.Finish()
	collected.Sent = uint64(t.w.moutq.Sent())

	// collect aggregate values
	aggrs := make(map[string]interface{})
	for name, aggr := range t.w.aggregators {
		aggrs[name] = aggr.ReduceAndEmit()
	}

	log.Printf("Superstep %d complete", t.Superstep)

	return &SuperstepTaskResponse{Info: collected, Errors: errors, Aggrs: aggrs}, nil
}

type WriteTask struct {
	batter.TaskerBase
	waffleTaskBase
}

type WriteTaskResponse struct {
	batter.TaskerBase
	Error error
}

func (t *WriteTask) Execute() (batter.TaskResponse, error) {
	t.w.minq.Drain()
	if t.w.resultWriter != nil {
		if error := t.w.resultWriter.WriteResults(t.w); error != nil {
			return &WriteTaskResponse{Error: error}, nil
		}
	} else {
		log.Printf("worker %s has no resultWriter", t.w.WorkerId())
	}
	return &WriteTaskResponse{}, nil
}
