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

func (p *GobPersister) PersistPartition(partitionId, superstep uint64, vertices []Vertex, inbound []Msg) error {
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
	log.Printf("Encoded partition %d (%d vertices, %d inbound messages) on superstep %d to %s", partitionId, len(vertices), len(inbound), superstep, filePath)
	return nil
}

func (p *GobPersister) LoadPartition(partitionId, superstep uint64) ([]Vertex, []Msg, error) {
	filePath := path.Join(p.path, strconv.Uitoa64(partitionId), strconv.Uitoa64(superstep), strconv.Uitoa64(superstep))
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
	log.Printf("Decoded partition %d (%d vertices, %d messages) on superstep %d from %s", partitionId, len(data.Vertices), len(data.Inbound), superstep, filePath)
	return data.Vertices, data.Inbound, nil
}

const master = "master"

func (p *GobPersister) PersistMaster(superstep uint64, partitions map[uint64]string) error {
	directory := path.Join(p.path, master)
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
	if err := enc.Encode(partitions); err != nil {
		return err
	}
	log.Printf("Encoded master info to for superstep %d (%d partitions) to %s", superstep, len(partitions), filePath)
	return nil
}

func (p *GobPersister) LoadMaster(superstep uint64) (map[uint64]string, error) {
	filePath := path.Join(p.path, master, strconv.Uitoa64(superstep))
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	partitions := make(map[uint64]string)
	if err := dec.Decode(partitions); err != nil {
		return nil, err
	}
	return partitions, nil
}
