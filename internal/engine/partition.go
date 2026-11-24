package engine

import (
	"sync"
	"unsafe"

	"woodagg/internal/agg"
	"woodagg/internal/model"
)

func ApproxKeyBytes(k model.Key) int64 {
	n := 0
	for i := 0; i < k.N; i++ {
		c := k.Cols[i]
		n += 2 + 8 // kind/null and fixed-width value/length header
		if c.Kind == model.KindString && !c.Null {
			n += len(c.Str)
		}
	}
	return int64(n)
}

func ApproxRowBytes(r agg.Row) int64 {
	n := ApproxKeyBytes(r.K) + int64(agg.MaxNumeric*8)
	for _, c := range r.DVals {
		n += int64(16 + len(c.Str))
	}
	return n
}

func ApproxStateBytes(s *agg.State) int64 {
	if s == nil {
		return 0
	}
	n := int64(unsafe.Sizeof(*s))
	for _, m := range s.Distinct {
		n += int64(len(m)) * int64(unsafe.Sizeof(model.Col{})+24)
		for c := range m {
			n += int64(len(c.Str))
		}
	}
	for _, t := range s.Top {
		if t != nil {
			n += int64(cap(t.Items)) * 8
		}
	}
	return n
}

func ApproxPartialBytes(k model.Key, s *agg.State) int64 {
	return ApproxKeyBytes(k) + ApproxStateBytes(s)
}

type Partition struct {
	ID int

	mu           sync.Mutex
	table        map[model.Key]*agg.State
	messages     int64
	sourceRows   int64
	bytes        int64
	streamGroups int64
}

func newPartition(id int) *Partition {
	return newPartitionHint(id, 0)
}

func newPartitionHint(id, hint int) *Partition {
	return &Partition{ID: id, table: make(map[model.Key]*agg.State, hint)}
}

func newPartitionSet(n int) []*Partition {
	return newPartitionSetHint(n, 0)
}

func newPartitionSetHint(n, hintPerPartition int) []*Partition {
	ps := make([]*Partition, n)
	for i := range ps {
		ps[i] = newPartitionHint(i, hintPerPartition)
	}
	return ps
}

func (p *Partition) mergeRow(r agg.Row, plan *agg.Plan) bool {
	p.mu.Lock()
	st := p.table[r.K]
	hit := st != nil
	if st == nil {
		st = plan.NewState()
		p.table[r.K] = st
	}
	plan.AddRow(st, r)
	p.messages++
	p.sourceRows++
	p.bytes += ApproxRowBytes(r)
	p.mu.Unlock()
	return hit
}

// mergeOwned takes ownership of st when the key is new. The caller must not
// continue mutating that state after this call.
func (p *Partition) mergeOwned(k model.Key, st *agg.State, plan *agg.Plan) {
	msgBytes := ApproxPartialBytes(k, st)
	p.mu.Lock()
	dst := p.table[k]
	if dst == nil {
		p.table[k] = st
	} else {
		plan.Merge(dst, st)
	}
	p.messages++
	p.sourceRows += st.Count
	p.bytes += msgBytes
	p.mu.Unlock()
}

func (p *Partition) addStreamRows(rows, bytes int64) {
	p.mu.Lock()
	p.messages += rows
	p.sourceRows += rows
	p.bytes += bytes
	p.mu.Unlock()
}

func (p *Partition) setStreamGroups(n int64) {
	p.mu.Lock()
	p.streamGroups = n
	p.mu.Unlock()
}

func (p *Partition) Snapshot() PartitionSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PartitionSnapshot{
		ID: p.ID, Groups: len(p.table) + int(p.streamGroups), Messages: p.messages,
		SourceRows: p.sourceRows, Bytes: p.bytes,
	}
}

func rescalePartitions(old []*Partition, n int, plan *agg.Plan) []*Partition {
	totalGroups := 0
	for _, op := range old {
		op.mu.Lock()
		totalGroups += len(op.table)
		op.mu.Unlock()
	}
	hint := totalGroups/n + 1
	fresh := newPartitionSetHint(n, hint)
	for _, op := range old {
		op.mu.Lock()
		for k, st := range op.table {
			idx := int(k.Hash() % uint64(n))
			np := fresh[idx]
			np.mu.Lock()
			dst := np.table[k]
			if dst == nil {
				np.table[k] = st
			} else {
				plan.Merge(dst, st)
			}
			np.messages++
			np.sourceRows += st.Count
			np.bytes += ApproxPartialBytes(k, st)
			np.mu.Unlock()
		}
		op.mu.Unlock()
	}
	return fresh
}
