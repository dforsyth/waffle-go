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

func NewGobPersister(path string) *GobPersister {
	return &GobPersister{
		path: path,
	}
}

func (p *GobPersister) filePath(w *Worker) string {
	return path.Join(p.path, w.WorkerId(), strconv.Uitoa64(w.Superstep()))
}

func (p *GobPersister) Write(w *Worker) error {
	filePath := p.filePath(w)
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(w); err != nil {
		return err
	}
	log.Printf("Encoded to %s", filePath)
	return nil
}

func (p *GobPersister) Read(w *Worker) error {
	filePath := p.filePath(w)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(f)
	if err := dec.Decode(w); err != nil {
		return err
	}
	log.Printf("Decoded from %s", filePath)
	return nil
}
