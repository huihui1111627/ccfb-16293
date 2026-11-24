package bench

import (
	"context"
	"runtime"
	"time"

	"woodagg/internal/data"
	"woodagg/internal/engine"
)

type ModeResult struct {
	Mode                engine.Mode       `json:"mode"`
	DurationMS          int64             `json:"duration_ms"`
	RowsPerSecond       float64           `json:"rows_per_second"`
	Groups              int               `json:"groups"`
	Rows                int64             `json:"rows"`
	StartHeapAlloc      uint64            `json:"start_heap_alloc"`
	PeakHeap            uint64            `json:"peak_heap"`
	PeakHeapDelta       uint64            `json:"peak_heap_delta"`
	TotalAllocDelta     uint64            `json:"total_alloc_delta"`
	HeapAfterGC         uint64            `json:"heap_after_gc"`
	EffectivePartitions int               `json:"effective_partitions"`
	Sorted              bool              `json:"sorted"`
	Decisions           map[string]int    `json:"decisions"`
	TopGroups           []engine.TopGroup `json:"top_groups"`
}

type Report struct {
	ScenarioID   string       `json:"scenario_id"`
	ScenarioName string       `json:"scenario_name"`
	StartedAt    time.Time    `json:"started_at"`
	Results      []ModeResult `json:"results"`
}

func Run(ctx context.Context, sc data.Scenario, rt *engine.RuntimeConfig, mon *engine.Monitor) (*Report, error) {
	modes := []engine.Mode{engine.ModeAdaptive, engine.ModeAlwaysPre, engine.ModeNeverPre}
	out := &Report{ScenarioID: sc.ID, ScenarioName: sc.Name, StartedAt: time.Now()}
	for _, mode := range modes {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}

		runtime.GC()
		runtime.GC()
		res, err := engine.Run(ctx, engine.Options{
			Scenario: sc, Mode: mode, Config: rt, Monitor: mon,
		})
		if err != nil {
			return out, err
		}
		r := res.Report
		mr := ModeResult{
			Mode: r.Mode, DurationMS: r.DurationMS, Groups: r.Groups, Rows: r.Rows,
			StartHeapAlloc: r.StartHeapAlloc, PeakHeap: r.PeakHeap, PeakHeapDelta: r.PeakHeapDelta,
			TotalAllocDelta: r.TotalAllocDelta, HeapAfterGC: r.HeapAfterGC,
			EffectivePartitions: r.EffectivePartitions, Sorted: r.Sorted,
			Decisions: r.Decisions, TopGroups: r.TopGroups,
		}
		if r.DurationMS > 0 {
			mr.RowsPerSecond = float64(r.Rows) / (float64(r.DurationMS) / 1000.0)
		}
		out.Results = append(out.Results, mr)

		// Release the full partition maps before measuring/running the next mode.
		res = nil
		r = nil
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	return out, nil
}
