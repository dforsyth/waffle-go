package waffle

import (
	"batter"
	"log"
	"sync"
)

type WaffleTask interface {
	SetWorker(*Worker)
}

type WaffleTaskBase struct {
	w *Worker
}

func (t *WaffleTaskBase) SetWorker(w *Worker) {
	t.w = w
}

func (t *WaffleTaskBase) TaskName() string {
	return "waffle"
}

type LoadTask struct {
	batter.TaskerBase
	WaffleTaskBase
	PartitionMap map[uint64]string
	Assignments  map[string][]string // maybe just bring this down to a []string.  does every worker really need to know what the other loaded?
}

type LoadTaskResponse struct {
	batter.TaskerBase
	Errors      []error
	TotalLoaded uint64
}

func (t *LoadTask) Execute() (batter.TaskResponse, error) {
	// set the partition map
	t.w.partitionMap = t.PartitionMap

	var assigned []string
	var ok bool
	// w is set on the way in
	if assigned, ok = t.Assignments[t.w.WorkerId()]; !ok {
		log.Printf("no load assignments for %s", t.w.WorkerId())
		return &LoadTaskResponse{}, nil
	}

	var totalLoaded uint64
	for _, assignment := range assigned {
		loaded, err := t.w.loader.Load(t.w, assignment)
		if err != nil {
			return &LoadTaskResponse{Errors: []error{err}}, nil
		}
		totalLoaded += loaded
	}

	// flush vertices and voutq
	t.w.FlushVertices()
	errors := t.w.voutq.Finish()

	return &LoadTaskResponse{TotalLoaded: totalLoaded, Errors: errors}, nil
}

type DistributeTask struct {
	batter.TaskerBase
	WaffleTaskBase
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

type SuperstepTask struct {
	batter.TaskerBase
	WaffleTaskBase
	Superstep  uint64
	Checkpoint bool
	Aggr       map[string]interface{}
}

type SuperstepTaskResponse struct {
	batter.TaskerBase
	Errors []error
	Info   *stepInfo
	Aggr   map[string]interface{}
}

func (t *SuperstepTask) Execute() (batter.TaskResponse, error) {
	if t.Checkpoint {
		// persist
	}

	var pWait sync.WaitGroup
	for _, p := range t.w.partitions {
		pp := p
		pWait.Add(1)
		go func() {
			pp.compute()
			pWait.Done()
		}()
	}
	pWait.Wait()
	// drain and restart moutq
	errors := t.w.moutq.Finish()
	t.w.moutq.Start()

	collected := &stepInfo{}
	/*
		for info := range <-collectCh {
			collected = infoMerge(collected, info)
		}
	*/
	return &SuperstepTaskResponse{Info: collected, Errors: errors, Aggr: nil}, nil
}

type WriteTask struct {
	batter.TaskerBase
	WaffleTaskBase
}

type WriteTaskResponse struct {
	batter.TaskerBase
	Error error
}

func (t *WriteTask) Execute() (batter.TaskResponse, error) {
	if t.w.resultWriter != nil {
		if error := t.w.resultWriter.WriteResults(t.w); error != nil {
			return &WriteTaskResponse{Error: error}, nil
		}
	} else {
		log.Printf("worker %s has no resultWriter", t.w.WorkerId())
	}
	return &WriteTaskResponse{}, nil
}
