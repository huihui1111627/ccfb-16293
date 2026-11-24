package check

import (
	"fmt"
	"math"
	"runtime"
	"strings"

	"woodagg/internal/agg"
	"woodagg/internal/data"
	"woodagg/internal/engine"
	"woodagg/internal/model"
)

type NaiveResult struct {
	Rows         []agg.Row
	Groups       map[model.Key]*agg.State
	Materialized int
}

// RunNaive is the oracle: deterministically materialize every row in memory,
// then aggregate on one goroutine into one ordinary map.
func RunNaive(sc data.Scenario, specs []agg.Spec) *NaiveResult {
	if specs == nil {
		specs = agg.DefaultSpecs()
	}
	rows := data.Materialize(sc)
	plan := agg.NewPlan(specs)
	groups := make(map[model.Key]*agg.State, sc.Groups)
	for i := range rows {
		r := rows[i]
		st := groups[r.K]
		if st == nil {
			st = plan.NewState()
			groups[r.K] = st
		}
		plan.AddRow(st, r)
	}
	return &NaiveResult{Rows: rows, Groups: groups, Materialized: len(rows)}
}

// ReleaseMaterializedRows drops the full row slice after the oracle map has
// been built. The oracle still performed the required full in-memory load;
// releasing it before the engine run leaves room for a second full group map.
func (n *NaiveResult) ReleaseMaterializedRows() {
	n.Rows = nil
	runtime.GC()
}

type Report struct {
	OK             bool     `json:"ok"`
	ScenarioID     string   `json:"scenario_id"`
	Mode           string   `json:"mode"`
	NaiveGroups    int      `json:"naive_groups"`
	EngineGroups   int      `json:"engine_groups"`
	CheckedGroups  int      `json:"checked_groups"`
	Materialized   int      `json:"materialized_rows"`
	MismatchCount  int      `json:"mismatch_count"`
	Mismatches     []string `json:"mismatches"`
	MaxFloatRelErr float64  `json:"max_float_rel_err"`
}

func Compare(sc data.Scenario, result *engine.Result, naive *NaiveResult) Report {
	specs := agg.DefaultSpecs()
	if result != nil && result.Plan != nil {
		specs = result.Plan.Specs
	}
	rep := Report{
		OK: true, ScenarioID: sc.ID, Mode: string(result.Report.Mode),
		NaiveGroups: len(naive.Groups), EngineGroups: result.Groups(),
		Materialized: naive.Materialized,
	}
	// No "seen" set is needed on the success path: if both maps have the same
	// cardinality and every engine key has an oracle state, the key sets are
	// identical. This avoids a third tens-of-millions-key map during checking.
	result.Visit(func(k model.Key, got *agg.State) {
		want, ok := naive.Groups[k]
		if !ok {
			rep.addMismatchf("engine has extra group %s", FormatKey(k))
			return
		}
		rep.CheckedGroups++
		compareState(k, got, want, specs, &rep)
	})
	if rep.EngineGroups != rep.NaiveGroups {
		rep.addMismatchf("group cardinality differs: engine=%d naive=%d", rep.EngineGroups, rep.NaiveGroups)
	}
	if rep.MismatchCount > 0 || rep.NaiveGroups != rep.EngineGroups {
		rep.OK = false
	}
	if len(rep.Mismatches) > 20 {
		rep.Mismatches = rep.Mismatches[:20]
	}
	return rep
}

func (r *Report) addMismatchf(format string, args ...any) {
	r.MismatchCount++
	if len(r.Mismatches) < 20 {
		r.Mismatches = append(r.Mismatches, fmt.Sprintf(format, args...))
	}
}

func compareState(key model.Key, got, want *agg.State, specs []agg.Spec, rep *Report) {
	// Hot path: avoid boxing every numeric result into interface{} for tens of
	// millions of groups. Only the rare TopN check uses FinalValue.
	for _, sp := range specs {
		switch sp.Op {
		case agg.OpCount:
			if got.Count != want.Count {
				rep.addMismatchf("%s %s: got %d want %d", FormatKey(key), sp.Alias, got.Count, want.Count)
			}
		case agg.OpDistinct:
			gi, wi := distinctCount(got, sp.DistinctIndex), distinctCount(want, sp.DistinctIndex)
			if gi != wi {
				rep.addMismatchf("%s %s: got %d want %d", FormatKey(key), sp.Alias, gi, wi)
			}
		case agg.OpSum:
			compareFloat(key, sp, got.Sum[sp.NumIndex], want.Sum[sp.NumIndex], rep)
		case agg.OpAvg:
			var gf, wf float64
			if got.Count != 0 {
				gf = got.Sum[sp.NumIndex] / float64(got.Count)
			}
			if want.Count != 0 {
				wf = want.Sum[sp.NumIndex] / float64(want.Count)
			}
			compareFloat(key, sp, gf, wf, rep)
		case agg.OpMin:
			if got.Count == 0 || want.Count == 0 || !floatClose(got.Min[sp.NumIndex], want.Min[sp.NumIndex]) {
				rep.addMismatchf("%s %s: got %v want %v", FormatKey(key), sp.Alias, got.Min[sp.NumIndex], want.Min[sp.NumIndex])
			}
		case agg.OpMax:
			if got.Count == 0 || want.Count == 0 || !floatClose(got.Max[sp.NumIndex], want.Max[sp.NumIndex]) {
				rep.addMismatchf("%s %s: got %v want %v", FormatKey(key), sp.Alias, got.Max[sp.NumIndex], want.Max[sp.NumIndex])
			}
		case agg.OpTopN:
			gv := agg.FinalValue(got, sp)
			wv := agg.FinalValue(want, sp)
			gt, _ := gv.([]float64)
			wt, _ := wv.([]float64)
			if len(gt) != len(wt) {
				rep.addMismatchf("%s %s length: got %v want %v", FormatKey(key), sp.Alias, gt, wt)
				continue
			}
			for i := range gt {
				if gt[i] != wt[i] {
					rep.addMismatchf("%s %s[%d]: got %v want %v", FormatKey(key), sp.Alias, i, gt[i], wt[i])
				}
			}
		}
	}
}

func distinctCount(s *agg.State, i int) int {
	if !s.DistinctHas[i] {
		return 0
	}
	if s.Distinct[i] != nil {
		return len(s.Distinct[i])
	}
	return 1
}

func compareFloat(key model.Key, sp agg.Spec, gf, wf float64, rep *Report) {
	if !floatClose(gf, wf) {
		rep.addMismatchf("%s %s: got %v want %v", FormatKey(key), sp.Alias, gf, wf)
		return
	}
	denom := math.Max(1, math.Max(math.Abs(gf), math.Abs(wf)))
	if e := math.Abs(gf-wf) / denom; e > rep.MaxFloatRelErr {
		rep.MaxFloatRelErr = e
	}
}

func floatClose(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	if a == b {
		return true
	}
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	return math.Abs(a-b) <= 1e-9*scale
}

func FormatKey(k model.Key) string {
	parts := make([]string, k.N)
	for i := 0; i < k.N; i++ {
		parts[i] = k.Cols[i].Display()
	}
	return "(" + strings.Join(parts, ", ") + ")"
}
