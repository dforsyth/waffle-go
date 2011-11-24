package waffle

type Persister interface {
	PersistPartition(uint64, uint64, []Vertex, []Msg) error
	LoadPartition(uint64, uint64) ([]Vertex, []Msg, error)
	// The master parts of the interface will take a map of partitions->address because that fits my current
	// model.  Really, they should take a slice of partitionIds, or even just a count of partitions if I can
	// gaurantee that there will be no gaps in the partition count
	PersistMaster(uint64, map[uint64]string) error
	LoadMaster(uint64) (map[uint64]string, error)
}
