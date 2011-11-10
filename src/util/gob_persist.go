package waffle

import (
	"gob"
	"log"
	"os"
	"path"
	"strconv"
)

// XXX doesn't check is path exists
// XXX This doesn't work because the most important fields in my workers are not public (duh?).  Will need to fix
// that before this Persister becomes useful...

type GobPersister struct {
	path string
}

type checkpointData struct {
	PartitionId, Superstep uint64
	Vertices               []Vertex
	Inbound                []Msg
}

func NewGobPersister(path string) *GobPersister {
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

func (p *GobPersister) filePath(partitionId, superstep uint64) string {
	return path.Join(p.path, strconv.Uitoa64(partitionId), strconv.Uitoa64(superstep))
}

func (p *GobPersister) Write(partitionId, superstep uint64, vertices []Vertex, inbound []Msg) error {
	filePath := p.filePath(partitionId, superstep)
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

func (p *GobPersister) Read(partitionId, superstep uint64) error {
	filePath := p.filePath(partitionId, superstep)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	data := &checkpointData{}
	if err := dec.Decode(data); err != nil {
		return err
	}
	log.Printf("Decoded from %s", filePath)
	return nil
}
