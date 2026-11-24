package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"woodagg/internal/engine"
)

func TestConfigPatchMergesPartialJSON(t *testing.T) {
	s := New()
	before := s.cfg.Get()

	req := httptest.NewRequest(http.MethodPost, "/api/config", bytes.NewBufferString(`{"threads":3}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	var got engine.Config
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Threads != 3 {
		t.Fatalf("threads=%d, want 3", got.Threads)
	}
	if got.LocalTableMax != before.LocalTableMax {
		t.Fatalf("local table max reset: got %d want %d", got.LocalTableMax, before.LocalTableMax)
	}
	if got.Partitions != before.Partitions {
		t.Fatalf("partitions reset: got %d want %d", got.Partitions, before.Partitions)
	}
	if got.AutoPartitions != before.AutoPartitions {
		t.Fatalf("auto partitions reset: got %v want %v", got.AutoPartitions, before.AutoPartitions)
	}
	if got.BatchRows != before.BatchRows {
		t.Fatalf("batch rows reset: got %d want %d", got.BatchRows, before.BatchRows)
	}
}
