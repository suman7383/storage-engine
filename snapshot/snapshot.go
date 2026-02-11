package snapshot

type Snapshot struct {
	seq uint64
}

func NewSnapshot(seq uint64) Snapshot {
	return Snapshot{
		seq: seq,
	}
}

func (s Snapshot) Seq() uint64 {
	return s.seq
}
