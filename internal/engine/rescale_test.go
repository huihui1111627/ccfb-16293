package engine

import (
	"testing"

	"woodagg/internal/agg"
	"woodagg/internal/model"
)

func TestRescalePartitionsPreservesEveryGroupAndState(t *testing.T) {
	plan := agg.NewPlan(agg.DefaultSpecs())
	old := newPartitionSet(2)
	const n = 500
	for i := 0; i < n; i++ {
		r := agg.Row{
			K:    model.K2(model.IntCol(int64(i)), model.StringCol("k")),
			Nums: [agg.MaxNumeric]float64{float64(i), 1, 2, 0},
		}
		// Deliberately assign without hash routing; rescale must reroute safely.
		old[i%2].mergeRow(r, plan)
	}

	reshaped := rescalePartitions(old, 7, plan)
	groups := 0
	var count int64
	var sum float64
	for _, p := range reshaped {
		p.mu.Lock()
		for _, st := range p.table {
			groups++
			count += st.Count
			sum += st.Sum[agg.NumAmount]
		}
		p.mu.Unlock()
	}
	if groups != n {
		t.Fatalf("groups after reshard = %d, want %d", groups, n)
	}
	if count != n {
		t.Fatalf("count after reshard = %d, want %d", count, n)
	}
	wantSum := float64(n*(n-1)) / 2
	if sum != wantSum {
		t.Fatalf("sum after reshard = %v, want %v", sum, wantSum)
	}
}
