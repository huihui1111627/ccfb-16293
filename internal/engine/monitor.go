package engine

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const MaxWorkers = 32

type Mode string

const (
	ModeAdaptive  Mode = "adaptive"
	ModeAlwaysPre Mode = "always_pre"
	ModeNeverPre  Mode = "never_pre"
)

type Decision string

const (
	DecisionSampling Decision = "sampling"
	DecisionPreAgg   Decision = "preagg"
	DecisionPass     Decision = "passthrough"
	DecisionSingle   Decision = "single"
	DecisionSorted   Decision = "sorted"
)

type Config struct {
	Threads         int     `json:"threads"`
	LocalTableMax   int     `json:"local_table_max"`
	Partitions      int     `json:"partitions"`
	AutoPartitions  bool    `json:"auto_partitions"`
	HighUniqueRatio float64 `json:"high_unique_ratio"`
	BatchRows       int     `json:"batch_rows"`
}

func DefaultConfig() Config {
	threads := runtime.NumCPU()
	if threads > MaxWorkers {
		threads = MaxWorkers
	}
	if threads < 1 {
		threads = 1
	}
	return Config{
		Threads:         threads,
		LocalTableMax:   50_000,
		Partitions:      8,
		AutoPartitions:  true,
		HighUniqueRatio: 0.72,
		BatchRows:       100_000,
	}
}

func (c Config) clamp() Config {
	if c.Threads < 1 {
		c.Threads = 1
	}
	if c.Threads > MaxWorkers {
		c.Threads = MaxWorkers
	}
	if c.LocalTableMax < 1 {
		c.LocalTableMax = 1
	}
	if c.LocalTableMax > 20_000_000 {
		c.LocalTableMax = 20_000_000
	}
	if c.Partitions < 1 {
		c.Partitions = 1
	}
	if c.Partitions > 1024 {
		c.Partitions = 1024
	}
	if c.HighUniqueRatio <= 0 || c.HighUniqueRatio > 1 {
		c.HighUniqueRatio = 0.72
	}
	if c.BatchRows < 1_000 {
		c.BatchRows = 1_000
	}
	if c.BatchRows > 1_000_000 {
		c.BatchRows = 1_000_000
	}
	return c
}

type RuntimeConfig struct {
	mu  sync.RWMutex
	cfg Config
}

func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{cfg: DefaultConfig()}
}

func (r *RuntimeConfig) Get() Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg.clamp()
}

func (r *RuntimeConfig) Update(fn func(*Config)) Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(&r.cfg)
	r.cfg = r.cfg.clamp()
	return r.cfg
}

func (r *RuntimeConfig) Set(c Config) {
	r.mu.Lock()
	r.cfg = c.clamp()
	r.mu.Unlock()
}

const (
	workerIdle = iota
	workerSampling
	workerPreAgg
	workerPass
	workerSingle
	workerSorted
	workerFlush
)

func workerStateName(v int32) string {
	switch v {
	case workerSampling:
		return "sampling"
	case workerPreAgg:
		return "preagg"
	case workerPass:
		return "passthrough"
	case workerSingle:
		return "single"
	case workerSorted:
		return "sorted"
	case workerFlush:
		return "flushing"
	default:
		return "idle"
	}
}

type WorkerStats struct {
	ID        int `json:"id"`
	state     atomic.Int32
	lastState atomic.Int32

	TableSize atomic.Int64 `json:"table_size"`
	Hits      atomic.Int64 `json:"hits"`
	Lookups   atomic.Int64 `json:"lookups"`
	Rows      atomic.Int64 `json:"rows"`
	Batches   atomic.Int64 `json:"batches"`
	Flushes   atomic.Int64 `json:"flushes"`
}

func (w *WorkerStats) setState(s int32) {
	w.state.Store(s)
	if s != workerIdle {
		w.lastState.Store(s)
	}
}

type WorkerSnapshot struct {
	ID        int     `json:"id"`
	State     string  `json:"state"`
	LastState string  `json:"last_state"`
	TableSize int64   `json:"table_size"`
	Hits      int64   `json:"hits"`
	Lookups   int64   `json:"lookups"`
	HitRate   float64 `json:"hit_rate"`
	Rows      int64   `json:"rows"`
	Batches   int64   `json:"batches"`
	Flushes   int64   `json:"flushes"`
}

func (w *WorkerStats) Snapshot() WorkerSnapshot {
	hits := w.Hits.Load()
	lookups := w.Lookups.Load()
	rate := 0.0
	if lookups > 0 {
		rate = float64(hits) / float64(lookups)
	}
	return WorkerSnapshot{
		ID: w.ID, State: workerStateName(w.state.Load()),
		LastState: workerStateName(w.lastState.Load()),
		TableSize: w.TableSize.Load(), Hits: hits, Lookups: lookups, HitRate: rate,
		Rows: w.Rows.Load(), Batches: w.Batches.Load(), Flushes: w.Flushes.Load(),
	}
}

type BatchStat struct {
	ID               int     `json:"id"`
	Rows             int     `json:"rows"`
	Sampled          int     `json:"sampled"`
	Unique           int     `json:"unique"`
	Singletons       int     `json:"singletons"`
	Doubletons       int     `json:"doubletons"`
	EstimatedInBatch int64   `json:"estimated_in_batch"`
	GlobalEstimate   int64   `json:"global_estimate"`
	UniqueRatio      float64 `json:"unique_ratio"`
	DistinctRatio    float64 `json:"distinct_ratio"`
	Decision         string  `json:"decision"`
	Reason           string  `json:"reason"`
	DurationMS       float64 `json:"duration_ms"`
}

type MemoryPoint struct {
	ElapsedMS    int64  `json:"elapsed_ms"`
	HeapAlloc    uint64 `json:"heap_alloc"`
	HeapSys      uint64 `json:"heap_sys"`
	TotalAlloc   uint64 `json:"total_alloc"`
	TrackedBytes int64  `json:"tracked_bytes"`
}

type PartitionSnapshot struct {
	ID         int     `json:"id"`
	Groups     int     `json:"groups"`
	Messages   int64   `json:"messages"`
	SourceRows int64   `json:"source_rows"`
	Bytes      int64   `json:"bytes"`
	Share      float64 `json:"share"`
}

type TopGroup struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
}

type Report struct {
	ScenarioID          string         `json:"scenario_id"`
	ScenarioName        string         `json:"scenario_name"`
	Mode                Mode           `json:"mode"`
	Groups              int            `json:"groups"`
	Rows                int64          `json:"rows"`
	DurationMS          int64          `json:"duration_ms"`
	StartHeapAlloc      uint64         `json:"start_heap_alloc"`
	PeakHeap            uint64         `json:"peak_heap"`
	PeakHeapDelta       uint64         `json:"peak_heap_delta"`
	StartTotalAlloc     uint64         `json:"start_total_alloc"`
	TotalAllocDelta     uint64         `json:"total_alloc_delta"`
	HeapAfterGC         uint64         `json:"heap_after_gc"`
	EffectivePartitions int            `json:"effective_partitions"`
	Sorted              bool           `json:"sorted"`
	Decisions           map[string]int `json:"decisions"`
	TopGroups           []TopGroup     `json:"top_groups"`
}

type Snapshot struct {
	Running             bool                `json:"running"`
	Phase               string              `json:"phase"`
	ScenarioID          string              `json:"scenario_id"`
	ScenarioName        string              `json:"scenario_name"`
	Mode                Mode                `json:"mode"`
	StartedAtUnixMS     int64               `json:"started_at_unix_ms"`
	ElapsedMS           int64               `json:"elapsed_ms"`
	Config              Config              `json:"config"`
	EffectivePartitions int                 `json:"effective_partitions"`
	ResultGroups        int64               `json:"result_groups"`
	EmittedGroups       int64               `json:"emitted_groups"`
	Workers             []WorkerSnapshot    `json:"workers"`
	Batches             []BatchStat         `json:"batches"`
	Partitions          []PartitionSnapshot `json:"partitions"`
	Memory              []MemoryPoint       `json:"memory"`
	Report              *Report             `json:"report,omitempty"`
	Error               string              `json:"error,omitempty"`
}

type Monitor struct {
	Config *RuntimeConfig

	mu            sync.Mutex
	running       bool
	phase         string
	scenarioID    string
	scenarioName  string
	mode          Mode
	startedAt     time.Time
	runID         int
	errMsg        string
	report        *Report
	resultGroups  int64
	emittedGroups atomic.Int64

	workers [MaxWorkers]*WorkerStats
	batches []BatchStat
	memory  []MemoryPoint

	finalPartitions []PartitionSnapshot
	partMu          sync.RWMutex
	partitions      []*Partition
}

func NewMonitor(cfg *RuntimeConfig) *Monitor {
	m := &Monitor{Config: cfg}
	for i := range m.workers {
		m.workers[i] = &WorkerStats{ID: i}
	}
	return m
}

func (m *Monitor) Worker(i int) *WorkerStats { return m.workers[i] }

func (m *Monitor) ResetRun(scenarioID, scenarioName string, mode Mode, cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runID++
	m.running = true
	m.phase = "starting"
	m.scenarioID = scenarioID
	m.scenarioName = scenarioName
	m.mode = mode
	m.startedAt = time.Now()
	m.errMsg = ""
	m.report = nil
	m.resultGroups = 0
	m.emittedGroups.Store(0)
	m.batches = m.batches[:0]
	m.memory = m.memory[:0]
	m.finalPartitions = nil
	for _, w := range m.workers {
		w.state.Store(workerIdle)
		w.lastState.Store(workerIdle)
		w.TableSize.Store(0)
		w.Hits.Store(0)
		w.Lookups.Store(0)
		w.Rows.Store(0)
		w.Batches.Store(0)
		w.Flushes.Store(0)
	}
	m.partMu.Lock()
	m.partitions = nil
	m.partMu.Unlock()
}

func (m *Monitor) Finish(report *Report, err error) {
	finalSnaps := m.partitionSnapshots()
	m.mu.Lock()
	m.running = false
	m.phase = "done"
	if err != nil {
		m.phase = "error"
		m.errMsg = err.Error()
	}
	m.report = report
	if report != nil {
		m.resultGroups = int64(report.Groups)
	}
	m.finalPartitions = finalSnaps
	for _, w := range m.workers {
		w.state.Store(workerIdle)
		w.TableSize.Store(0)
	}
	m.mu.Unlock()

	// Keep only the compact snapshots for the page; the large partition maps
	// are owned by the returned Result and can be released by the caller.
	m.partMu.Lock()
	m.partitions = nil
	m.partMu.Unlock()
}

func (m *Monitor) SetPhase(p string) {
	m.mu.Lock()
	m.phase = p
	m.mu.Unlock()
}

func (m *Monitor) AddBatch(b BatchStat) {
	m.mu.Lock()
	m.batches = append(m.batches, b)
	if len(m.batches) > 100 {
		m.batches = append([]BatchStat(nil), m.batches[len(m.batches)-100:]...)
	}
	m.mu.Unlock()
}

func (m *Monitor) AddMemory(p MemoryPoint) {
	m.mu.Lock()
	m.memory = append(m.memory, p)
	if len(m.memory) > 6000 {
		m.memory = append([]MemoryPoint(nil), m.memory[len(m.memory)-6000:]...)
	}
	m.mu.Unlock()
}

func (m *Monitor) SetResultGroups(n int) {
	atomic.StoreInt64(&m.resultGroups, int64(n))
}

func (m *Monitor) setPartitions(p []*Partition) {
	m.partMu.Lock()
	m.partitions = p
	m.partMu.Unlock()
}

func (m *Monitor) partitionSnapshots() []PartitionSnapshot {
	m.partMu.RLock()
	ps := append([]*Partition(nil), m.partitions...)
	m.partMu.RUnlock()
	out := make([]PartitionSnapshot, 0, len(ps))
	var totalBytes int64
	snaps := make([]PartitionSnapshot, len(ps))
	for i, p := range ps {
		s := p.Snapshot()
		snaps[i] = s
		totalBytes += s.Bytes
	}
	for _, s := range snaps {
		if totalBytes > 0 {
			s.Share = float64(s.Bytes) / float64(totalBytes)
		}
		out = append(out, s)
	}
	return out
}

func (m *Monitor) Snapshot() Snapshot {
	cfg := m.Config.Get()
	m.mu.Lock()
	snap := Snapshot{
		Running: m.running, Phase: m.phase, ScenarioID: m.scenarioID,
		ScenarioName: m.scenarioName, Mode: m.mode,
		StartedAtUnixMS: m.startedAt.UnixMilli(), Config: cfg,
		ResultGroups:  atomic.LoadInt64(&m.resultGroups),
		EmittedGroups: m.emittedGroups.Load(),
		Batches:       append([]BatchStat(nil), m.batches...),
		Memory:        append([]MemoryPoint(nil), m.memory...),
		Report:        m.report, Error: m.errMsg,
	}
	var finalParts []PartitionSnapshot
	if len(m.finalPartitions) > 0 {
		finalParts = append([]PartitionSnapshot(nil), m.finalPartitions...)
	}
	if !m.startedAt.IsZero() {
		if snap.Running {
			snap.ElapsedMS = time.Since(m.startedAt).Milliseconds()
		} else if m.report != nil {
			snap.ElapsedMS = m.report.DurationMS
		}
	}
	m.mu.Unlock()
	for _, w := range m.workers {
		snap.Workers = append(snap.Workers, w.Snapshot())
	}
	snap.Partitions = m.partitionSnapshots()
	if len(snap.Partitions) == 0 {
		snap.Partitions = finalParts
	}
	snap.EffectivePartitions = len(snap.Partitions)
	return snap
}
