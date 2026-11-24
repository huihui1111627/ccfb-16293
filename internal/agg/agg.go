package agg

import (
	"sort"

	"woodagg/internal/model"
)

const (
	NumAmount = iota
	NumQuantity
	NumPrice
	MaxNumeric = 4

	DistinctTicket = 0
	MaxDistinct    = 2
	MaxTop         = 2
)

type Op string

const (
	OpSum      Op = "sum"
	OpCount    Op = "count"
	OpMin      Op = "min"
	OpMax      Op = "max"
	OpAvg      Op = "avg"
	OpDistinct Op = "distinct"
	OpTopN     Op = "topn"
)

type Spec struct {
	Op            Op     `json:"op"`
	Alias         string `json:"alias"`
	NumIndex      int    `json:"num_index,omitempty"`
	DistinctIndex int    `json:"distinct_index,omitempty"`
	TopIndex      int    `json:"top_index,omitempty"`
	N             int    `json:"n,omitempty"`
}

type Plan struct {
	Specs       []Spec `json:"specs"`
	HasDistinct [MaxDistinct]bool
	HasTop      [MaxTop]bool
}

func NewPlan(specs []Spec) *Plan {
	p := &Plan{Specs: specs}
	for _, sp := range specs {
		switch sp.Op {
		case OpDistinct:
			if sp.DistinctIndex >= 0 && sp.DistinctIndex < MaxDistinct {
				p.HasDistinct[sp.DistinctIndex] = true
			}
		case OpTopN:
			if sp.TopIndex >= 0 && sp.TopIndex < MaxTop {
				p.HasTop[sp.TopIndex] = true
			}
		}
	}
	return p
}

func DefaultSpecs() []Spec {
	return []Spec{
		{Op: OpCount, Alias: "检尺条数"},
		{Op: OpSum, Alias: "材积总和", NumIndex: NumAmount},
		{Op: OpSum, Alias: "件数总和", NumIndex: NumQuantity},
		{Op: OpAvg, Alias: "平均价格", NumIndex: NumPrice},
		{Op: OpMin, Alias: "最小件数", NumIndex: NumQuantity},
		{Op: OpMax, Alias: "最大件数", NumIndex: NumQuantity},
		{Op: OpDistinct, Alias: "不同检尺单号", DistinctIndex: DistinctTicket},
	}
}

// TopNSpecs is used by focused tests/queries for the per-group top-values
// aggregate. The default dashboard query instead exposes final top groups.
func TopNSpecs() []Spec {
	return append(DefaultSpecs(), Spec{
		Op: OpTopN, Alias: "最大三笔材积", NumIndex: NumAmount, TopIndex: 0, N: 3,
	})
}

type Row struct {
	K     model.Key
	Nums  [MaxNumeric]float64
	DVals [MaxDistinct]model.Col
}

type TopK struct {
	N     int       `json:"n"`
	Items []float64 `json:"items"` // min-heap while accumulating
}

func NewTopK(n int) *TopK {
	if n <= 0 {
		n = 1
	}
	return &TopK{N: n, Items: make([]float64, 0, n)}
}

func (t *TopK) Add(v float64) {
	if len(t.Items) < t.N {
		t.Items = append(t.Items, v)
		topUp(t.Items, len(t.Items)-1)
		return
	}
	if t.N > 0 && v > t.Items[0] {
		t.Items[0] = v
		topDown(t.Items, 0)
	}
}

func (t *TopK) SortedDescending() []float64 {
	out := append([]float64(nil), t.Items...)
	sort.Sort(sort.Reverse(sort.Float64Slice(out)))
	return out
}

func topUp(h []float64, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if h[p] <= h[i] {
			break
		}
		h[p], h[i] = h[i], h[p]
		i = p
	}
}

func topDown(h []float64, i int) {
	n := len(h)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		j := l
		if r := l + 1; r < n && h[r] < h[l] {
			j = r
		}
		if h[i] <= h[j] {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

// State is the mergeable two-phase aggregate. Avg remains (Sum, Count) and
// distinct count remains a set until FinalValue is called.
type State struct {
	Count int64
	Sum   [MaxNumeric]float64
	Min   [MaxNumeric]float64
	Max   [MaxNumeric]float64

	// DistinctFirst is the common one-element fast path. A map is allocated
	// only after a second, different value appears.
	DistinctFirst [MaxDistinct]model.Col
	DistinctHas   [MaxDistinct]bool
	Distinct      [MaxDistinct]map[model.Col]struct{}

	Top [MaxTop]*TopK
}

func (p *Plan) NewState() *State {
	return &State{}
}

func (p *Plan) ensureTop(s *State, index int) {
	if s.Top[index] == nil {
		n := 1
		for _, sp := range p.Specs {
			if sp.Op == OpTopN && sp.TopIndex == index && sp.N > 0 {
				n = sp.N
				break
			}
		}
		s.Top[index] = NewTopK(n)
	}
}

func (p *Plan) AddRow(s *State, r Row) {
	s.Count++
	first := s.Count == 1
	for i := 0; i < MaxNumeric-1; i++ {
		v := r.Nums[i]
		s.Sum[i] += v
		if first {
			s.Min[i] = v
			s.Max[i] = v
		} else {
			if v < s.Min[i] {
				s.Min[i] = v
			}
			if v > s.Max[i] {
				s.Max[i] = v
			}
		}
	}
	for i := 0; i < MaxDistinct; i++ {
		if !p.HasDistinct[i] {
			continue
		}
		v := r.DVals[i]
		addDistinct(s, i, v)
	}
	for i := 0; i < MaxTop; i++ {
		if p.HasTop[i] {
			p.ensureTop(s, i)
			s.Top[i].Add(r.Nums[topField(p, i)])
		}
	}
}

func addDistinct(s *State, i int, v model.Col) {
	if !s.DistinctHas[i] {
		s.DistinctHas[i] = true
		s.DistinctFirst[i] = v
		return
	}
	if s.Distinct[i] == nil {
		if s.DistinctFirst[i] == v {
			return
		}
		s.Distinct[i] = map[model.Col]struct{}{
			s.DistinctFirst[i]: {},
			v:                  {},
		}
		return
	}
	s.Distinct[i][v] = struct{}{}
}

func topField(p *Plan, topIndex int) int {
	for _, sp := range p.Specs {
		if sp.Op == OpTopN && sp.TopIndex == topIndex {
			return sp.NumIndex
		}
	}
	return NumAmount
}

// Merge combines an intermediate local state into a partition state.
func (p *Plan) Merge(dst, src *State) {
	dst.Count += src.Count
	for i := 0; i < MaxNumeric-1; i++ {
		dst.Sum[i] += src.Sum[i]
		if src.Count > 0 {
			if dst.Count == src.Count {
				dst.Min[i] = src.Min[i]
				dst.Max[i] = src.Max[i]
			} else {
				if src.Min[i] < dst.Min[i] {
					dst.Min[i] = src.Min[i]
				}
				if src.Max[i] > dst.Max[i] {
					dst.Max[i] = src.Max[i]
				}
			}
		}
	}
	for i := 0; i < MaxDistinct; i++ {
		if !p.HasDistinct[i] || !src.DistinctHas[i] {
			continue
		}
		if !dst.DistinctHas[i] {
			dst.DistinctHas[i] = true
			dst.DistinctFirst[i] = src.DistinctFirst[i]
			if src.Distinct[i] != nil {
				dst.Distinct[i] = make(map[model.Col]struct{}, len(src.Distinct[i]))
				for v := range src.Distinct[i] {
					dst.Distinct[i][v] = struct{}{}
				}
			}
			continue
		}
		if src.Distinct[i] == nil {
			addDistinct(dst, i, src.DistinctFirst[i])
			continue
		}
		// Ensure dst's singleton value is represented before unioning maps.
		if dst.Distinct[i] == nil {
			dst.Distinct[i] = map[model.Col]struct{}{dst.DistinctFirst[i]: {}}
		}
		for v := range src.Distinct[i] {
			dst.Distinct[i][v] = struct{}{}
		}
	}
	for i := 0; i < MaxTop; i++ {
		if !p.HasTop[i] || src.Top[i] == nil || dst.Top[i] == nil {
			continue
		}
		for _, v := range src.Top[i].Items {
			dst.Top[i].Add(v)
		}
	}
}

func FinalValue(s *State, sp Spec) any {
	switch sp.Op {
	case OpCount:
		return s.Count
	case OpSum:
		return s.Sum[sp.NumIndex]
	case OpAvg:
		if s.Count == 0 {
			return nil
		}
		return s.Sum[sp.NumIndex] / float64(s.Count)
	case OpMin:
		if s.Count == 0 {
			return nil
		}
		return s.Min[sp.NumIndex]
	case OpMax:
		if s.Count == 0 {
			return nil
		}
		return s.Max[sp.NumIndex]
	case OpDistinct:
		if !s.DistinctHas[sp.DistinctIndex] {
			return 0
		}
		if s.Distinct[sp.DistinctIndex] != nil {
			return len(s.Distinct[sp.DistinctIndex])
		}
		return 1
	case OpTopN:
		if s.Top[sp.TopIndex] == nil {
			return []float64{}
		}
		return s.Top[sp.TopIndex].SortedDescending()
	default:
		return nil
	}
}

func FinalValues(s *State, specs []Spec) []any {
	out := make([]any, len(specs))
	for i, sp := range specs {
		out[i] = FinalValue(s, sp)
	}
	return out
}
