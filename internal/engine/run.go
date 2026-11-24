package engine

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"woodagg/internal/agg"
	"woodagg/internal/data"
	"woodagg/internal/model"
)

const (
	SmallGroupThreshold      = 256
	TargetGroupsPerPartition = 100_000
	MaxAutoPartitions        = 512
)

var errUnsorted = errors.New("input is not globally sorted by key")

type Options struct {
	Scenario data.Scenario
	Mode     Mode
	Config   *RuntimeConfig
	Monitor  *Monitor
	Specs    []agg.Spec
}

type SortedGroup struct {
	Key   model.Key
	State *agg.State
}

type Result struct {
	Report       *Report
	Plan         *agg.Plan
	Partitions   []*Partition
	SortedGroups []SortedGroup
	Sorted       bool
}

func (r *Result) Groups() int {
	if r.Sorted {
		return len(r.SortedGroups)
	}
	n := 0
	for _, p := range r.Partitions {
		p.mu.Lock()
		n += len(p.table)
		p.mu.Unlock()
	}
	return n
}

func (r *Result) Visit(fn func(model.Key, *agg.State)) {
	if r.Sorted {
		for _, g := range r.SortedGroups {
			fn(g.Key, g.State)
		}
		return
	}
	for _, p := range r.Partitions {
		p.mu.Lock()
		for k, st := range p.table {
			fn(k, st)
		}
		p.mu.Unlock()
	}
}

type execResult struct {
	partitions   []*Partition
	sortedGroups []SortedGroup
	sorted       bool
	rows         int64
	decisions    map[string]int
	partCount    int
}

func Run(ctx context.Context, opts Options) (*Result, error) {
	rt := opts.Config
	if rt == nil {
		rt = NewRuntimeConfig()
	}
	cfg := rt.Get()
	specs := opts.Specs
	if specs == nil {
		specs = agg.DefaultSpecs()
	}
	plan := agg.NewPlan(specs)
	mon := opts.Monitor
	if mon == nil {
		mon = NewMonitor(rt)
	}
	mon.ResetRun(opts.Scenario.ID, opts.Scenario.Name, opts.Mode, cfg)

	runtime.GC()
	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)
	start := time.Now()
	var peak atomic.Uint64
	stopSampler := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopSampler:
				sampleMemory(mon, start, &peak)
				return
			case <-t.C:
				sampleMemory(mon, start, &peak)
			}
		}
	}()

	exec, err := execute(ctx, opts.Scenario, opts.Mode, rt, mon, plan)
	close(stopSampler)
	samplerWG.Wait()
	if err != nil {
		rep := &Report{ScenarioID: opts.Scenario.ID, Mode: opts.Mode}
		mon.Finish(rep, err)
		return nil, err
	}

	groups := 0
	if exec.sorted {
		groups = len(exec.sortedGroups)
	} else {
		for _, p := range exec.partitions {
			p.mu.Lock()
			groups += len(p.table)
			p.mu.Unlock()
		}
	}
	mon.SetResultGroups(groups)
	topGroups := buildTopGroups(exec, 10)

	elapsed := time.Since(start)
	runtime.GC()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)
	peakHeap := peak.Load()
	var peakDelta uint64
	if peakHeap > startMem.HeapAlloc {
		peakDelta = peakHeap - startMem.HeapAlloc
	}
	rep := &Report{
		ScenarioID: opts.Scenario.ID, ScenarioName: opts.Scenario.Name, Mode: opts.Mode,
		Groups: groups, Rows: exec.rows, DurationMS: elapsed.Milliseconds(),
		StartHeapAlloc: startMem.HeapAlloc, PeakHeap: peakHeap, PeakHeapDelta: peakDelta,
		StartTotalAlloc: startMem.TotalAlloc,
		TotalAllocDelta: endMem.TotalAlloc - startMem.TotalAlloc,
		HeapAfterGC:     endMem.HeapAlloc, EffectivePartitions: exec.partCount,
		Sorted: exec.sorted, Decisions: exec.decisions, TopGroups: topGroups,
	}
	mon.Finish(rep, nil)
	return &Result{
		Report: rep, Plan: plan, Partitions: exec.partitions,
		SortedGroups: exec.sortedGroups, Sorted: exec.sorted,
	}, nil
}

func sampleMemory(mon *Monitor, start time.Time, peak *atomic.Uint64) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	for {
		cur := peak.Load()
		if ms.HeapAlloc <= cur || peak.CompareAndSwap(cur, ms.HeapAlloc) {
			break
		}
	}
	tracked := int64(0)
	for _, p := range mon.partitionSnapshots() {
		tracked += p.Bytes
	}
	for _, w := range mon.workers {
		tracked += w.TableSize.Load() * 512
	}
	mon.AddMemory(MemoryPoint{
		ElapsedMS: time.Since(start).Milliseconds(), HeapAlloc: ms.HeapAlloc,
		HeapSys: ms.HeapSys, TotalAlloc: ms.TotalAlloc, TrackedBytes: tracked,
	})
}

func execute(ctx context.Context, sc data.Scenario, mode Mode, rt *RuntimeConfig, mon *Monitor, plan *agg.Plan) (*execResult, error) {
	if mode == ModeAdaptive {
		mon.SetPhase("detect_sorted_order")
		if probeSorted(ctx, sc) {
			sorted, err := runSorted(ctx, sc, rt, mon, plan)
			if err == nil {
				return sorted, nil
			}
			if !errors.Is(err, errUnsorted) {
				return nil, err
			}
			// The bounded probe saw order, but the full adjacent-key scan found
			// an inversion. Restart with the normal hash path; the fixed-seed
			// source can be recreated deterministically.
			mon.ResetRun(sc.ID, sc.Name, mode, rt.Get())
		}
	}
	return runHash(ctx, sc, mode, rt, mon, plan)
}

// probeSorted performs a bounded adjacent-key check before committing to the
// no-hash streaming path. runSorted still verifies the complete input.
func probeSorted(ctx context.Context, sc data.Scenario) bool {
	src := data.NewSource(sc)
	checked := 0
	var prev model.Key
	havePrev := false
	for checked < 100_000 {
		if err := ctx.Err(); err != nil {
			return false
		}
		batch := src.NextBatch(16_384)
		if len(batch) == 0 {
			return true
		}
		for _, r := range batch {
			if havePrev && model.CompareKey(r.K, prev) < 0 {
				return false
			}
			prev = r.K
			havePrev = true
			checked++
			if checked >= 100_000 {
				return true
			}
		}
	}
	return true
}

func runHash(ctx context.Context, sc data.Scenario, mode Mode, rt *RuntimeConfig, mon *Monitor, plan *agg.Plan) (*execResult, error) {
	cfg := rt.Get()
	src := data.NewSource(sc)
	first := src.NextBatch(cfg.BatchRows)
	decisions := map[string]int{}
	if first == nil {
		parts := newPartitionSet(cfg.Partitions)
		mon.setPartitions(parts)
		return &execResult{partitions: parts, decisions: decisions, partCount: len(parts)}, nil
	}

	est0 := EstimateBatch(first, 16_384, sc.Rows)
	if mode == ModeAdaptive && est0.ChaoDistinct <= float64(SmallGroupThreshold) {
		return runSingle(ctx, first, src, est0, sc, mode, rt, mon, plan, decisions)
	}

	partCount := cfg.Partitions
	autoPart := 0
	hintPerPartition := 1024
	if cfg.AutoPartitions {
		autoPart = partitionsForEstimate(est0)
		partCount = maxInt(partCount, autoPart)
	}
	if est0.GlobalEstimate > 0 {
		hintPerPartition = maxInt(64, int(est0.GlobalEstimate/int64(partCount))+1)
	}
	parts := newPartitionSetHint(partCount, hintPerPartition)
	mon.setPartitions(parts)

	var rows int64
	batchID := 0
	worstGlobal := est0.GlobalEstimate

	processOne := func(batch []agg.Row, est Estimate) error {
		batchStart := time.Now()
		cfgNow := rt.Get()
		if cfgNow.AutoPartitions {
			autoPart = maxInt(autoPart, partitionsForEstimate(est))
			want := maxInt(cfgNow.Partitions, autoPart)
			if want != len(parts) {
				parts = rescalePartitions(parts, want, plan)
				mon.setPartitions(parts)
			}
		} else if cfgNow.Partitions != len(parts) {
			parts = rescalePartitions(parts, cfgNow.Partitions, plan)
			mon.setPartitions(parts)
		}

		decision, reason := chooseDecision(mode, est, cfgNow)
		threads := cfgNow.Threads
		if threads > len(batch) {
			threads = maxInt(1, len(batch))
		}
		err := processBatchParallel(ctx, batch, threads, decision, parts, rt, mon, plan, batchID)
		if err != nil {
			return err
		}
		d := string(decision)
		decisions[d]++
		rows += int64(len(batch))
		mon.AddBatch(BatchStat{
			ID: batchID, Rows: len(batch), Sampled: est.SampleSize, Unique: est.Unique,
			Singletons: est.Singletons, Doubletons: est.Doubletons,
			EstimatedInBatch: est.EstimatedInBatch, GlobalEstimate: est.GlobalEstimate,
			UniqueRatio: est.UniqueRatio, DistinctRatio: est.DistinctRatio,
			Decision: d, Reason: reason, DurationMS: float64(time.Since(batchStart).Microseconds()) / 1000.0,
		})
		batchID++
		worstGlobal = max64(worstGlobal, est.GlobalEstimate)
		return nil
	}

	if err := processOne(first, est0); err != nil {
		return nil, err
	}
	for {
		cfgNow := rt.Get()
		batch := src.NextBatch(cfgNow.BatchRows)
		if batch == nil {
			break
		}
		est := EstimateBatch(batch, 16_384, sc.Rows)
		if err := processOne(batch, est); err != nil {
			return nil, err
		}
	}
	return &execResult{partitions: parts, rows: rows, decisions: decisions, partCount: len(parts)}, nil
}

func processBatchParallel(
	ctx context.Context,
	batch []agg.Row,
	threads int,
	decision Decision,
	parts []*Partition,
	rt *RuntimeConfig,
	mon *Monitor,
	plan *agg.Plan,
	batchID int,
) error {
	var wg sync.WaitGroup
	var cancelErr atomic.Bool
	for t := 0; t < threads; t++ {
		start := t * len(batch) / threads
		end := (t + 1) * len(batch) / threads
		if start == end {
			continue
		}
		w := mon.Worker(t)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if decision == DecisionPreAgg {
				err = runPreChunk(ctx, batch[start:end], w, parts, rt, plan)
			} else {
				err = runPassChunk(ctx, batch[start:end], w, parts, plan)
			}
			if err != nil {
				cancelErr.Store(true)
			}
		}()
	}
	wg.Wait()
	if cancelErr.Load() {
		return ctx.Err()
	}
	return ctx.Err()
}

type localTable struct {
	m map[model.Key]*agg.State
}

func runPreChunk(
	ctx context.Context,
	chunk []agg.Row,
	w *WorkerStats,
	parts []*Partition,
	rt *RuntimeConfig,
	plan *agg.Plan,
) error {
	w.setState(workerPreAgg)
	defer w.setState(workerIdle)
	w.Batches.Add(1)

	cfg := rt.Get()
	hint := cfg.LocalTableMax
	if h := len(chunk); h < hint {
		hint = h
	}
	lt := &localTable{m: make(map[model.Key]*agg.State, hint)}
	maxEntries := cfg.LocalTableMax

	var hits, lookups, rows int64
	var lastHits, lastLookups, lastRows int64
	flushStats := func(force bool) {
		if force || lookups-lastLookups >= 4096 {
			w.Hits.Add(hits - lastHits)
			w.Lookups.Add(lookups - lastLookups)
			w.Rows.Add(rows - lastRows)
			lastHits, lastLookups, lastRows = hits, lookups, rows
			w.TableSize.Store(int64(len(lt.m)))
		}
	}

	for i := range chunk {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				flushStats(true)
				flushLocalTable(lt.m, parts, plan, w)
				lt.m = nil
				return err
			}
			maxEntries = rt.Get().LocalTableMax
		}
		r := chunk[i]
		rows++
		lookups++
		st, ok := lt.m[r.K]
		if ok {
			hits++
		} else {
			if len(lt.m) >= maxEntries {
				flushLocalTable(lt.m, parts, plan, w)
				w.Flushes.Add(1)
				lt.m = make(map[model.Key]*agg.State, maxInt(16, minInt(maxEntries, len(chunk)-i)))
			}
			st = plan.NewState()
			lt.m[r.K] = st
		}
		plan.AddRow(st, r)
		flushStats(false)
	}
	flushStats(true)
	flushLocalTable(lt.m, parts, plan, w)
	lt.m = nil
	w.TableSize.Store(0)
	return nil
}

func flushLocalTable(m map[model.Key]*agg.State, parts []*Partition, plan *agg.Plan, w *WorkerStats) {
	if w != nil {
		w.setState(workerFlush)
		defer w.setState(workerPreAgg)
	}
	for k, st := range m {
		idx := int(k.Hash() % uint64(len(parts)))
		parts[idx].mergeOwned(k, st, plan)
	}
}

func runPassChunk(
	ctx context.Context,
	chunk []agg.Row,
	w *WorkerStats,
	parts []*Partition,
	plan *agg.Plan,
) error {
	w.setState(workerPass)
	defer w.setState(workerIdle)
	w.Batches.Add(1)
	w.TableSize.Store(0)
	var rows int64
	var last int64
	for i := range chunk {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				w.Rows.Add(rows - last)
				return err
			}
		}
		r := chunk[i]
		idx := int(r.K.Hash() % uint64(len(parts)))
		parts[idx].mergeRow(r, plan)
		rows++
		if rows-last >= 4096 {
			w.Rows.Add(rows - last)
			last = rows
		}
	}
	w.Rows.Add(rows - last)
	return nil
}

func runSingle(
	ctx context.Context,
	first []agg.Row,
	src *data.Source,
	est0 Estimate,
	sc data.Scenario,
	mode Mode,
	rt *RuntimeConfig,
	mon *Monitor,
	plan *agg.Plan,
	decisions map[string]int,
) (*execResult, error) {
	p := newPartition(0)
	parts := []*Partition{p}
	mon.setPartitions(parts)
	w := mon.Worker(0)
	w.setState(workerSingle)
	defer w.setState(workerIdle)

	var rows int64
	batchID := 0
	handle := func(batch []agg.Row, est Estimate) error {
		start := time.Now()
		var lookups, hits, lastLookups, lastHits int64
		for i := range batch {
			if i&8191 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if p.mergeRow(batch[i], plan) {
				hits++
			}
			lookups++
			if lookups-lastLookups >= 4096 {
				w.Lookups.Add(lookups - lastLookups)
				w.Hits.Add(hits - lastHits)
				lastLookups, lastHits = lookups, hits
				w.TableSize.Store(int64(p.Snapshot().Groups))
			}
		}
		w.Lookups.Add(lookups - lastLookups)
		w.Hits.Add(hits - lastHits)
		rows += int64(len(batch))
		snap := p.Snapshot()
		w.TableSize.Store(int64(snap.Groups))
		w.Rows.Store(rows)
		w.Batches.Add(1)
		decisions[string(DecisionSingle)]++
		mon.AddBatch(BatchStat{
			ID: batchID, Rows: len(batch), Sampled: est.SampleSize, Unique: est.Unique,
			Singletons: est.Singletons, Doubletons: est.Doubletons,
			EstimatedInBatch: est.EstimatedInBatch, GlobalEstimate: est.GlobalEstimate,
			UniqueRatio: est.UniqueRatio, DistinctRatio: est.DistinctRatio,
			Decision: string(DecisionSingle), Reason: "估计分组很少，单线程汇总，避免 shuffle",
			DurationMS: float64(time.Since(start).Microseconds()) / 1000.0,
		})
		batchID++
		return nil
	}

	if err := handle(first, est0); err != nil {
		return nil, err
	}
	for {
		batch := src.NextBatch(rt.Get().BatchRows)
		if batch == nil {
			break
		}
		est := EstimateBatch(batch, 8192, sc.Rows)
		if err := handle(batch, est); err != nil {
			return nil, err
		}
	}
	return &execResult{partitions: parts, rows: rows, decisions: decisions, partCount: 1}, nil
}

func runSorted(
	ctx context.Context,
	sc data.Scenario,
	rt *RuntimeConfig,
	mon *Monitor,
	plan *agg.Plan,
) (*execResult, error) {
	src := data.NewSource(sc)
	p := newPartition(0)
	mon.setPartitions([]*Partition{p})
	w := mon.Worker(0)
	w.setState(workerSorted)
	defer w.setState(workerIdle)

	var out []SortedGroup
	var cur model.Key
	var st *agg.State
	has := false
	var rows int64
	batchID := 0

	finishGroup := func() {
		if !has {
			return
		}
		out = append(out, SortedGroup{Key: cur, State: st})
		mon.emittedGroups.Add(1)
		p.setStreamGroups(int64(len(out)))
	}

	for {
		batch := src.NextBatch(rt.Get().BatchRows)
		if batch == nil {
			break
		}
		start := time.Now()
		w.Batches.Add(1)
		var batchRows int64
		for i := range batch {
			if i&8191 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			r := batch[i]
			if !has {
				cur = r.K
				st = plan.NewState()
				has = true
			} else {
				cmp := model.CompareKey(r.K, cur)
				if cmp < 0 {
					return nil, errUnsorted
				}
				if cmp > 0 {
					finishGroup()
					cur = r.K
					st = plan.NewState()
				}
			}
			plan.AddRow(st, r)
			batchRows++
		}
		rows += batchRows
		w.Rows.Store(rows)
		w.TableSize.Store(int64(len(out)))
		p.addStreamRows(batchRows, ApproxRowBytes(batch[0])*int64(len(batch)))
		mon.AddBatch(BatchStat{
			ID: batchID, Rows: len(batch), Decision: string(DecisionSorted),
			Reason:     "相邻键全程有序，边读边输出，不建哈希表",
			DurationMS: float64(time.Since(start).Microseconds()) / 1000.0,
		})
		batchID++
	}
	finishGroup()
	return &execResult{sortedGroups: out, sorted: true, rows: rows, decisions: map[string]int{string(DecisionSorted): batchID}, partCount: 1}, nil
}

func buildTopGroups(e *execResult, n int) []TopGroup {
	var items []TopGroup
	consider := func(k model.Key, st *agg.State) {
		v := st.Sum[agg.NumAmount]
		g := TopGroup{Key: model.FormatKey(k), Value: v}
		if len(items) < n {
			items = append(items, g)
			sort.Slice(items, func(i, j int) bool { return items[i].Value < items[j].Value })
			return
		}
		if v > items[0].Value {
			items[0] = g
			sort.Slice(items, func(i, j int) bool { return items[i].Value < items[j].Value })
		}
	}
	if e.sorted {
		for _, g := range e.sortedGroups {
			consider(g.Key, g.State)
		}
	} else {
		for _, p := range e.partitions {
			p.mu.Lock()
			for k, st := range p.table {
				consider(k, st)
			}
			p.mu.Unlock()
		}
	}
	out := make([]TopGroup, len(items))
	for i := range items {
		out[len(items)-1-i] = items[i]
	}
	return out
}

func partitionsForEstimate(e Estimate) int {
	n := int((e.GlobalEstimate + TargetGroupsPerPartition - 1) / TargetGroupsPerPartition)
	if n < 1 {
		n = 1
	}
	return minInt(n, MaxAutoPartitions)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
