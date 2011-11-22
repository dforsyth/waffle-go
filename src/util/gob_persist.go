package waffle

import (
	"encoding/gob"
	"log"
	"os"
	"path"
	"strconv"
)

// XXX doesn't check is path exists
// This could probably just take an encoder and be generalized?

type GobPersister struct {
	path string
}

type checkpointData struct {
	PartitionId, Superstep uint64
	Vertices               []Vertex
	Inbound                []Msg
}

func NewGobPersister(path string) *GobPersister {
	if err := os.RemoveAll(path); err != nil {
		return nil
	}
	return &GobPersister{
		path: path,
	}
}

func newCheckpointData(partitionId, superstep uint64, vertices []Vertex, inbound []Msg) *checkpointData {
	return &checkpointData{
		PartitionId: partitionId,
		Superstep:   superstep,
		Vertices:    vertices,
		Inbound:     inbound,
	}
}

func (p *GobPersister) Persist(partitionId, superstep uint64, vertices []Vertex, inbound []Msg) error {
	directory := path.Join(p.path, strconv.Uitoa64(partitionId))
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	filePath := path.Join(directory, strconv.Uitoa64(superstep))
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	if err := enc.Encode(newCheckpointData(partitionId, superstep, vertices, inbound)); err != nil {
		return err
	}
	log.Printf("Encoded to %s", filePath)
	return nil
}

func (p *GobPersister) Load(partitionId, superstep uint64) ([]Vertex, []Msg, error) {
	filePath := path.Join(strconv.Uitoa64(partitionId), strconv.Uitoa64(superstep), strconv.Uitoa64(superstep))
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	data := &checkpointData{}
	if err := dec.Decode(data); err != nil {
		return nil, nil, err
	}
	log.Printf("Decoded from %s", filePath)
	return data.Vertices, data.Inbound, nil
}
