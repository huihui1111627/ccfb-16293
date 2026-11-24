package engine

import (
	"math"

	"woodagg/internal/agg"
	"woodagg/internal/model"
)

type Estimate struct {
	SampleSize       int
	Unique           int
	Singletons       int
	Doubletons       int
	ChaoDistinct     float64
	EstimatedInBatch int64
	GlobalEstimate   int64
	UniqueRatio      float64
	DistinctRatio    float64
}

// EstimateBatch inspects a systematic sample, not just a prefix. Chao1 is used
// to avoid treating a few observed hot groups as proof that the whole batch has
// low cardinality.
func EstimateBatch(batch []agg.Row, sampleWanted int, totalRows int) Estimate {
	n := len(batch)
	if sampleWanted <= 0 || sampleWanted > n {
		sampleWanted = n
	}
	stride := 1
	if n > sampleWanted {
		stride = n / sampleWanted
		if stride < 1 {
			stride = 1
		}
	}
	freq := make(map[model.Key]int, sampleWanted)
	checked := 0
	for i := 0; i < n; i += stride {
		freq[batch[i].K]++
		checked++
		if checked >= sampleWanted {
			break
		}
	}

	var single, double int
	for _, c := range freq {
		if c == 1 {
			single++
		} else if c == 2 {
			double++
		}
	}
	d := len(freq)
	chao := float64(d)
	if single > 0 {
		chao += float64(single*(single-1)) / (2 * float64(double+1))
	}
	if chao > float64(n) {
		chao = float64(n)
	}
	uniqueRatio := 0.0
	if checked > 0 {
		uniqueRatio = float64(d) / float64(checked)
	}
	distinctRatio := 0.0
	if n > 0 {
		distinctRatio = chao / float64(n)
		if distinctRatio > 1 {
			distinctRatio = 1
		}
	}
	global := int64(chao)
	if uniqueRatio >= 0.55 {
		global = int64(distinctRatio * float64(totalRows))
	}
	if global < int64(d) {
		global = int64(d)
	}
	return Estimate{
		SampleSize: checked, Unique: d, Singletons: single, Doubletons: double,
		ChaoDistinct: chao, EstimatedInBatch: int64(math.Round(chao)),
		GlobalEstimate: global, UniqueRatio: uniqueRatio, DistinctRatio: distinctRatio,
	}
}

func chooseDecision(mode Mode, e Estimate, cfg Config) (Decision, string) {
	if mode == ModeNeverPre {
		return DecisionPass, "策略=从不预聚合"
	}
	if mode == ModeAlwaysPre {
		return DecisionPreAgg, "策略=总是预聚合"
	}
	if e.UniqueRatio >= cfg.HighUniqueRatio {
		return DecisionPass, "样本唯一键占比高，预聚合命中率低"
	}
	if float64(e.EstimatedInBatch) > float64(cfg.LocalTableMax)*0.9 {
		return DecisionPass, "估计分组数接近/超过本地表上限"
	}
	return DecisionPreAgg, "重复分组足够多，本地预聚合可减少分区消息"
}
