package engine_test

import (
	"context"
	"math"
	"testing"

	"woodagg/internal/agg"
	"woodagg/internal/check"
	"woodagg/internal/data"
	"woodagg/internal/engine"
	"woodagg/internal/model"
)

func testScenarios() []data.Scenario {
	return []data.Scenario{
		{ID: "tiny-wood", Name: "tiny wood", Groups: 48, Rows: 40_000, Seed: 42, Schema: data.SchemaWood, NullRate: 0.05, SpecialRate: 0.03, BatchRows: 4096},
		{ID: "buyer-high", Name: "buyer high", Groups: 2_000, Rows: 30_000, Seed: 43, Schema: data.SchemaBuyer, NullRate: 0.03, BatchRows: 4096},
		{ID: "sorted", Name: "sorted", Groups: 800, Rows: 30_000, Seed: 44, Schema: data.SchemaSorted, Sorted: true, BatchRows: 4096},
		{ID: "long", Name: "long", Groups: 300, Rows: 15_000, Seed: 45, Schema: data.SchemaLongString, LongStrings: true, NullRate: 0.02, BatchRows: 4096},
	}
}

func TestAllModesMatchNaive(t *testing.T) {
	for _, sc := range testScenarios() {
		naive := check.RunNaive(sc, nil)
		for _, mode := range []engine.Mode{engine.ModeAdaptive, engine.ModeAlwaysPre, engine.ModeNeverPre} {
			t.Run(sc.ID+"/"+string(mode), func(t *testing.T) {
				cfg := engine.NewRuntimeConfig()
				cfg.Set(engine.Config{
					Threads: 4, LocalTableMax: 128, Partitions: 5,
					AutoPartitions: true, HighUniqueRatio: 0.72, BatchRows: 4096,
				})
				res, err := engine.Run(context.Background(), engine.Options{
					Scenario: sc, Mode: mode, Config: cfg,
				})
				if err != nil {
					t.Fatal(err)
				}
				rep := check.Compare(sc, res, naive)
				if !rep.OK {
					t.Fatalf("comparison failed groups=%d/%d mismatch=%d: %v maxFloatErr=%g",
						rep.EngineGroups, rep.NaiveGroups, rep.MismatchCount, rep.Mismatches, rep.MaxFloatRelErr)
				}
				t.Logf("groups=%d rows=%d maxFloatErr=%g decisions=%v",
					rep.EngineGroups, res.Report.Rows, rep.MaxFloatRelErr, res.Report.Decisions)
			})
		}
	}
}

func TestTopNAggregateMatchesNaive(t *testing.T) {
	sc := data.Scenario{
		ID: "topn", Groups: 120, Rows: 20_000, Seed: 99,
		Schema: data.SchemaWood, NullRate: 0.01, SpecialRate: 0.01, BatchRows: 2048,
	}
	specs := agg.TopNSpecs()
	naive := check.RunNaive(sc, specs)
	cfg := engine.NewRuntimeConfig()
	cfg.Set(engine.Config{Threads: 4, LocalTableMax: 64, Partitions: 3, BatchRows: 2048, HighUniqueRatio: 0.99})
	res, err := engine.Run(context.Background(), engine.Options{
		Scenario: sc, Mode: engine.ModeAlwaysPre, Config: cfg, Specs: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	rep := check.Compare(sc, res, naive)
	if !rep.OK {
		t.Fatalf("topn mismatch: %v", rep.Mismatches)
	}
}

func TestFloatKeyGroupingRules(t *testing.T) {
	negZero := model.FloatCol(math.Copysign(0, -1))
	posZero := model.FloatCol(0)
	nan1 := model.FloatCol(math.NaN())
	nan2 := model.FloatCol(math.Float64frombits(0x7ff8000000000007))
	if negZero != posZero {
		t.Fatal("-0 and +0 must be equal Col values")
	}
	if nan1 != nan2 {
		t.Fatal("all NaN payloads must be normalized to one Col")
	}
	if model.K1(negZero).Hash() != model.K1(posZero).Hash() {
		t.Fatal("zero hashes differ")
	}
	if model.K1(nan1).Hash() != model.K1(nan2).Hash() {
		t.Fatal("nan hashes differ")
	}
}
