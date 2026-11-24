package server

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"sync"
	"sync/atomic"

	"woodagg/internal/bench"
	"woodagg/internal/check"
	"woodagg/internal/data"
	"woodagg/internal/engine"
	webfs "woodagg/web"
)

type Server struct {
	cfg     *engine.RuntimeConfig
	monitor *engine.Monitor

	busy    atomic.Bool
	jobKind atomic.Value // string

	mu        sync.Mutex
	bench     *bench.Report
	verify    *check.Report
	lastError string
}

func New() *Server {
	cfg := engine.NewRuntimeConfig()
	return &Server{cfg: cfg, monitor: engine.NewMonitor(cfg)}
}

type stateResponse struct {
	engine.Snapshot
	Scenarios []data.Scenario `json:"scenarios"`
	Busy      bool            `json:"busy"`
	JobKind   string          `json:"job_kind"`
	Benchmark *bench.Report   `json:"benchmark,omitempty"`
	Verify    *check.Report   `json:"verify,omitempty"`
	LastError string          `json:"last_error,omitempty"`
}

type runRequest struct {
	ScenarioID string      `json:"scenario_id"`
	Mode       engine.Mode `json:"mode"`
}

type verifyRequest struct {
	ScenarioID string      `json:"scenario_id"`
	Mode       engine.Mode `json:"mode"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/run", s.handleRun)
	mux.HandleFunc("/api/benchmark", s.handleBenchmark)
	mux.HandleFunc("/api/verify", s.handleVerify)

	staticSub, err := fs.Sub(webfs.FS, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticSub)))
	return mux
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	resp := stateResponse{
		Snapshot:  s.monitor.Snapshot(),
		Scenarios: data.DefaultScenarios(),
		Busy:      s.busy.Load(),
		JobKind:   jobKindString(s.jobKind.Load()),
		Benchmark: s.bench,
		Verify:    s.verify,
		LastError: s.lastError,
	}
	s.mu.Unlock()
	writeJSON(w, resp)
}

type configPatch struct {
	Threads         *int     `json:"threads"`
	LocalTableMax   *int     `json:"local_table_max"`
	Partitions      *int     `json:"partitions"`
	AutoPartitions  *bool    `json:"auto_partitions"`
	HighUniqueRatio *float64 `json:"high_unique_ratio"`
	BatchRows       *int     `json:"batch_rows"`
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var patch configPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	got := s.cfg.Update(func(c *engine.Config) {
		if patch.Threads != nil {
			c.Threads = *patch.Threads
		}
		if patch.LocalTableMax != nil {
			c.LocalTableMax = *patch.LocalTableMax
		}
		if patch.Partitions != nil {
			c.Partitions = *patch.Partitions
		}
		if patch.AutoPartitions != nil {
			c.AutoPartitions = *patch.AutoPartitions
		}
		if patch.HighUniqueRatio != nil {
			c.HighUniqueRatio = *patch.HighUniqueRatio
		}
		if patch.BatchRows != nil {
			c.BatchRows = *patch.BatchRows
		}
	})
	writeJSON(w, got)
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sc, ok := data.FindScenario(req.ScenarioID)
	if !ok {
		http.Error(w, "unknown scenario", http.StatusBadRequest)
		return
	}
	mode := req.Mode
	if mode == "" {
		mode = engine.ModeAdaptive
	}
	if !s.startJob("run") {
		http.Error(w, "a job is already running", http.StatusConflict)
		return
	}
	go func() {
		defer s.finishJob()
		_, err := engine.Run(context.Background(), engine.Options{
			Scenario: sc, Mode: mode, Config: s.cfg, Monitor: s.monitor,
		})
		s.setError(err)
	}()
	writeJSON(w, map[string]string{"status": "started"})
}

func (s *Server) handleBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req runRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.ScenarioID == "" {
		req.ScenarioID = "dozens"
	}
	sc, ok := data.FindScenario(req.ScenarioID)
	if !ok {
		http.Error(w, "unknown scenario", http.StatusBadRequest)
		return
	}
	if !s.startJob("benchmark") {
		http.Error(w, "a job is already running", http.StatusConflict)
		return
	}
	s.mu.Lock()
	s.bench = nil
	s.mu.Unlock()
	go func() {
		defer s.finishJob()
		rep, err := bench.Run(context.Background(), sc, s.cfg, s.monitor)
		s.mu.Lock()
		s.bench = rep
		s.mu.Unlock()
		s.setError(err)
	}()
	writeJSON(w, map[string]string{"status": "started"})
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req verifyRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.ScenarioID == "" {
		req.ScenarioID = "dozens"
	}
	sc, ok := data.FindScenario(req.ScenarioID)
	if !ok {
		http.Error(w, "unknown scenario", http.StatusBadRequest)
		return
	}
	mode := req.Mode
	if mode == "" {
		mode = engine.ModeAdaptive
	}
	if !s.startJob("verify") {
		http.Error(w, "a job is already running", http.StatusConflict)
		return
	}
	s.mu.Lock()
	s.verify = nil
	s.mu.Unlock()
	go func() {
		defer s.finishJob()
		naive := check.RunNaive(sc, nil)
		naive.ReleaseMaterializedRows()
		res, err := engine.Run(context.Background(), engine.Options{
			Scenario: sc, Mode: mode, Config: s.cfg, Monitor: s.monitor,
		})
		if err != nil {
			s.setError(err)
			return
		}
		rep := check.Compare(sc, res, naive)
		s.mu.Lock()
		s.verify = &rep
		s.mu.Unlock()
	}()
	writeJSON(w, map[string]string{"status": "started"})
}

func (s *Server) startJob(kind string) bool {
	if !s.busy.CompareAndSwap(false, true) {
		return false
	}
	s.jobKind.Store(kind)
	s.mu.Lock()
	s.lastError = ""
	s.mu.Unlock()
	return true
}

func (s *Server) finishJob() {
	s.busy.Store(false)
	s.jobKind.Store("")
}

func (s *Server) setError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
}

func jobKindString(v any) string {
	if v == nil {
		return ""
	}
	if x, ok := v.(string); ok {
		return x
	}
	return ""
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	if err := enc.Encode(v); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
