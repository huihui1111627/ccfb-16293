package engine

import (
	"strings"
	"testing"
	"unsafe"

	"woodagg/internal/agg"
	"woodagg/internal/model"
)

func TestStoredStringKeyKeepsOriginalBackingArray(t *testing.T) {
	long := "交易对手-" + strings.Repeat("原始字节", 512)
	originalPtr := unsafe.StringData(long)

	plan := agg.NewPlan(agg.DefaultSpecs())
	p := newPartition(0)
	key := model.K1(model.StringCol(long))
	p.mergeOwned(key, plan.NewState(), plan)

	for storedKey := range p.table {
		gotPtr := unsafe.StringData(storedKey.Cols[0].Str)
		if gotPtr != originalPtr {
			t.Fatalf("string backing bytes were copied: orig=%p stored=%p", originalPtr, gotPtr)
		}
		if storedKey.Hash() != key.Hash() {
			t.Fatal("hash changed")
		}
	}
}
