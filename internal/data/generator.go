package data

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strings"

	"woodagg/internal/agg"
	"woodagg/internal/model"
)

type Schema string

const (
	SchemaWood       Schema = "wood"
	SchemaBuyer      Schema = "buyer"
	SchemaSorted     Schema = "sorted"
	SchemaLongString Schema = "long"
)

type Scenario struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Groups      int     `json:"groups"`
	Rows        int     `json:"rows"`
	Seed        int64   `json:"seed"`
	Schema      Schema  `json:"schema"`
	Sorted      bool    `json:"sorted"`
	NullRate    float64 `json:"null_rate"`
	SpecialRate float64 `json:"special_rate"`
	LongStrings bool    `json:"long_strings"`
	BatchRows   int     `json:"batch_rows"`
}

func DefaultScenarios() []Scenario {
	return []Scenario{
		{ID: "dozens", Name: "几十组 · 树种/产地/径级", Groups: 36, Rows: 3_000_000, Seed: 20261125, Schema: SchemaWood, NullRate: 0.03, SpecialRate: 0.01, BatchRows: 100_000},
		{ID: "hundreds", Name: "几百组 · 明显重复", Groups: 256, Rows: 3_000_000, Seed: 20261125, Schema: SchemaWood, NullRate: 0.025, SpecialRate: 0.008, BatchRows: 100_000},
		{ID: "ten-thousand", Name: "一万组 · 混合", Groups: 10_000, Rows: 4_000_000, Seed: 20261125, Schema: SchemaWood, NullRate: 0.02, SpecialRate: 0.005, BatchRows: 100_000},
		{ID: "million", Name: "一百万组 · 高基数", Groups: 1_000_000, Rows: 5_000_000, Seed: 20261125, Schema: SchemaBuyer, NullRate: 0.015, BatchRows: 100_000},
		{ID: "ten-million", Name: "一千万组 · 买家近唯一", Groups: 10_000_000, Rows: 12_000_000, Seed: 20261125, Schema: SchemaBuyer, NullRate: 0.01, BatchRows: 100_000},
		{ID: "thirty-million", Name: "三千万组 · 极端基数", Groups: 30_000_000, Rows: 33_000_000, Seed: 20261125, Schema: SchemaBuyer, NullRate: 0.008, BatchRows: 100_000},
		{ID: "sorted", Name: "已按分组键排序", Groups: 200_000, Rows: 3_000_000, Seed: 20261125, Schema: SchemaSorted, Sorted: true, BatchRows: 100_000},
		{ID: "long-string", Name: "长字符串键 · 原始字节比较", Groups: 100_000, Rows: 1_200_000, Seed: 20261125, Schema: SchemaLongString, LongStrings: true, NullRate: 0.01, BatchRows: 100_000},
	}
}

func FindScenario(id string) (Scenario, bool) {
	for _, sc := range DefaultScenarios() {
		if sc.ID == id {
			return sc, true
		}
	}
	return Scenario{}, false
}

var (
	species = []string{"杉木", "松木", "桉木", "橡木", "樟木", "楠木", "桦木", "毛竹"}
	origins = []string{"建瓯", "顺昌", "永安", "武平", "开化", "都匀", "融水", "靖州", "绥宁", "江华", "崇义", "龙泉"}
	buyers  = []string{"闽林", "瓯源", "青山", "两江", "金杉", "绿海", "丰林", "远峰", "清溪", "南岭"}
	longPad = strings.Repeat("生态木材产地编码长名称", 24)
)

type Source struct {
	sc      Scenario
	rng     *rand.Rand
	emitted int

	// coverageStep is coprime to Groups, so multiplying the coverage index by
	// it is a deterministic permutation of every target group. This avoids an
	// artificially long sorted prefix while still guaranteeing coverage.
	coverageStep int64

	// sorted generator state
	sortedID     int64
	sortedRemain int64
	nullRemain   int64
}

func NewSource(sc Scenario) *Source {
	if sc.BatchRows <= 0 {
		sc.BatchRows = 100_000
	}
	return &Source{
		sc:           sc,
		rng:          rand.New(rand.NewPCG(uint64(sc.Seed), 0x9e3779b97f4a7c15)),
		coverageStep: coverageStep(int64(sc.Groups)),
		sortedID:     -1,
		nullRemain:   int64(maxInt(1, sc.Groups/100)),
	}
}

func (s *Source) Scenario() Scenario { return s.sc }

func (s *Source) NextBatch(limit int) []agg.Row {
	if limit <= 0 {
		limit = s.sc.BatchRows
	}
	if remaining := s.sc.Rows - s.emitted; remaining < limit {
		limit = maxInt(0, remaining)
	}
	if limit == 0 {
		return nil
	}
	batch := make([]agg.Row, 0, limit)
	for len(batch) < limit {
		if s.emitted >= s.sc.Rows {
			break
		}
		if s.sc.Sorted || s.sc.Schema == SchemaSorted {
			batch = append(batch, s.nextSortedRow())
		} else {
			batch = append(batch, s.nextRandomRow())
		}
		s.emitted++
	}
	return batch
}

func (s *Source) nextRandomRow() agg.Row {
	idx := s.emitted
	var id int64
	// One deterministic permutation/coverage pass means every target group exists.
	if idx < s.sc.Groups {
		g := int64(s.sc.Groups)
		if g > 1 {
			id = (int64(idx) * s.coverageStep) % g
		}
	} else {
		id = s.skewID()
	}
	k := s.keyForID(id)

	// Only inject unusual keys after the coverage pass. This preserves the
	// intended target groups while still adding null/special groups.
	if idx >= s.sc.Groups {
		k = s.injectUnusual(k)
	}

	ticketPool := int64(minInt(200_000, maxInt(64, s.sc.Groups*2)))
	ticket := s.rng.Int64N(ticketPool)
	return agg.Row{
		K: k,
		Nums: [agg.MaxNumeric]float64{
			0.05 + s.rng.Float64()*4.8,         // volume
			1 + math.Floor(s.rng.Float64()*24), // pieces
			780 + s.rng.Float64()*1450,         // price
			0,
		},
		DVals: [agg.MaxDistinct]model.Col{
			model.IntCol(100000 + ticket),
			{},
		},
	}
}

func coverageStep(g int64) int64 {
	if g <= 2 {
		return 1
	}
	step := int64(float64(g) * 0.61803398875)
	if step%2 == 0 {
		step--
	}
	for step > 1 && gcd64(step, g) != 1 {
		step -= 2
	}
	return maxInt64(1, step)
}

func gcd64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *Source) skewID() int64 {
	g := int64(s.sc.Groups)
	if g <= 1 {
		return 0
	}
	// Power distribution creates a small set of very hot buyers/groups.
	id := int64(math.Pow(s.rng.Float64(), 2.35) * float64(g))
	if id >= g {
		id = g - 1
	}
	return id
}

func (s *Source) keyForID(id int64) model.Key {
	switch s.sc.Schema {
	case SchemaBuyer:
		buyer := fmt.Sprintf("BY-%010d", id)
		region := origins[int(id%int64(len(origins)))]
		if s.sc.LongStrings {
			buyer += "-" + longPad
		}
		return model.K2(model.StringCol(buyer), model.StringCol(region))
	case SchemaLongString:
		code := fmt.Sprintf("LONG-%08d-%s", id, longPad)
		return model.K3(
			model.StringCol(species[int(id%int64(len(species)))]),
			model.StringCol(code),
			model.FloatCol(8+float64((id/int64(len(species)))%48)),
		)
	default: // wood
		sp := species[int(id%int64(len(species)))]
		or := origins[(id/int64(len(species)))%int64(len(origins))]
		diamIdx := (id / int64(len(species)*len(origins))) % 48
		diam := 8 + float64(diamIdx)*2.5
		bucket := id / int64(len(species)*len(origins)*48)
		originCol := or
		if s.sc.LongStrings {
			originCol = or + "-" + longPad
		}
		buyerCode := fmt.Sprintf("%s-%08d", buyers[bucket%int64(len(buyers))], bucket)
		return model.K4(
			model.StringCol(sp),
			model.StringCol(originCol),
			model.FloatCol(diam),
			model.StringCol(buyerCode),
		)
	}
}

func (s *Source) injectUnusual(k model.Key) model.Key {
	if s.rng.Float64() < s.sc.NullRate {
		return s.canonicalNullKey()
	}
	if s.rng.Float64() < s.sc.SpecialRate {
		return s.canonicalSpecialFloatKey(k)
	}
	return k
}

// canonicalNullKey keeps null examples in a few stable groups instead of
// creating a null variant for every ordinary key.
func (s *Source) canonicalNullKey() model.Key {
	switch s.sc.Schema {
	case SchemaBuyer:
		if s.rng.IntN(2) == 0 {
			return model.K2(model.NullCol(), model.StringCol("未知产地"))
		}
		return model.K2(model.StringCol("现金散客"), model.NullCol())
	case SchemaLongString:
		return model.K3(model.StringCol("杉木"), model.NullCol(), model.FloatCol(24))
	default:
		switch s.rng.IntN(3) {
		case 0:
			return model.K4(model.NullCol(), model.StringCol("建瓯"), model.FloatCol(20.5), model.StringCol("散客-00000000"))
		case 1:
			return model.K4(model.StringCol("杉木"), model.NullCol(), model.FloatCol(22.0), model.StringCol("散客-00000001"))
		default:
			return model.K4(model.StringCol("松木"), model.StringCol("顺昌"), model.NullCol(), model.StringCol("散客-00000002"))
		}
	}
}

func (s *Source) canonicalSpecialFloatKey(fallback model.Key) model.Key {
	var special float64
	switch s.rng.IntN(4) {
	case 0:
		special = math.Copysign(0, -1)
	case 1:
		special = 0
	case 2:
		special = math.NaN()
	default:
		special = math.Float64frombits(0x7ff8000000000007)
	}
	switch s.sc.Schema {
	case SchemaLongString:
		return model.K3(model.StringCol("杉木"), model.StringCol("特殊径级键"), model.FloatCol(special))
	case SchemaWood, "":
		return model.K4(model.StringCol("杉木"), model.StringCol("建瓯"), model.FloatCol(special), model.StringCol("特殊径级买家"))
	default:
		return fallback
	}
}

func (s *Source) nextSortedRow() agg.Row {
	var k model.Key
	if s.nullRemain > 0 {
		k = model.K1(model.NullCol())
		s.nullRemain--
	} else {
		if s.sortedRemain <= 0 {
			s.sortedID++
			// Hot early keys have longer runs.
			hot := 0.0
			if s.sortedID < int64(s.sc.Groups/10) {
				hot = 40 * math.Pow(1-float64(s.sortedID)/float64(maxInt(1, s.sc.Groups/10)), 2)
			}
			s.sortedRemain = 1 + int64(hot) + s.rng.Int64N(5)
		}
		id := s.sortedID
		if id >= int64(s.sc.Groups) {
			id = int64(s.sc.Groups) - 1
		}
		k = model.K1(model.StringCol(fmt.Sprintf("K-%010d", id)))
		s.sortedRemain--
	}

	ticket := s.rng.Int64N(128)
	return agg.Row{
		K: k,
		Nums: [agg.MaxNumeric]float64{
			0.05 + s.rng.Float64()*4.8,
			1 + math.Floor(s.rng.Float64()*24),
			780 + s.rng.Float64()*1450,
			0,
		},
		DVals: [agg.MaxDistinct]model.Col{model.IntCol(100000 + ticket), {}},
	}
}

func Materialize(sc Scenario) []agg.Row {
	src := NewSource(sc)
	rows := make([]agg.Row, 0, sc.Rows)
	for {
		b := src.NextBatch(sc.BatchRows)
		if b == nil {
			break
		}
		rows = append(rows, b...)
	}
	return rows
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
