package model

import (
	"math"
	"strconv"
	"strings"
)

const MaxKeyCols = 4

type ColKind uint8

const (
	KindNull ColKind = iota
	KindString
	KindInt
	KindFloat
)

// CanonicalNaNBits makes every NaN payload belong to the same group.
const CanonicalNaNBits uint64 = 0x7ff8000000000001

// Col is a comparable grouping-key column.
//
// For strings only the sixteen-byte string header is copied by the engine; the
// underlying byte array is never copied during probing or insertion. Hashing
// and ordering read the original bytes directly.
type Col struct {
	Str  string
	I64  int64
	Bits uint64
	Kind ColKind
	Null bool
}

func NullCol() Col {
	return Col{Kind: KindNull, Null: true}
}

func StringCol(s string) Col {
	return Col{Kind: KindString, Str: s}
}

func IntCol(v int64) Col {
	return Col{Kind: KindInt, I64: v}
}

func FloatCol(f float64) Col {
	if f == 0 { // +0 and -0 must hash and compare equally.
		return Col{Kind: KindFloat, Bits: 0}
	}
	if math.IsNaN(f) { // all NaN payloads are one group.
		return Col{Kind: KindFloat, Bits: CanonicalNaNBits}
	}
	return Col{Kind: KindFloat, Bits: math.Float64bits(f)}
}

func (c Col) Float() float64 {
	return math.Float64frombits(c.Bits)
}

func (c Col) Display() string {
	if c.Null {
		return "NULL"
	}
	switch c.Kind {
	case KindString:
		return c.Str
	case KindInt:
		return strconv.FormatInt(c.I64, 10)
	case KindFloat:
		f := c.Float()
		if math.IsNaN(f) {
			return "NaN"
		}
		if f == 0 {
			return "0"
		}
		return strconv.FormatFloat(f, 'f', -1, 64)
	default:
		return "?"
	}
}

type Key struct {
	Cols [MaxKeyCols]Col
	N    int
}

func K1(a Col) Key {
	return Key{N: 1, Cols: [MaxKeyCols]Col{a}}
}

func K2(a, b Col) Key {
	return Key{N: 2, Cols: [MaxKeyCols]Col{a, b}}
}

func K3(a, b, c Col) Key {
	return Key{N: 3, Cols: [MaxKeyCols]Col{a, b, c}}
}

func K4(a, b, c, d Col) Key {
	return Key{N: 4, Cols: [MaxKeyCols]Col{a, b, c, d}}
}

const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

func fnvByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	return h * fnvPrime
}

func fnvUint64(h uint64, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h = fnvByte(h, byte(v>>(8*i)))
	}
	return h
}

// Hash hashes key columns without converting a source string to []byte.
func (k Key) Hash() uint64 {
	h := fnvOffset
	h = fnvUint64(h, uint64(k.N))
	for i := 0; i < k.N; i++ {
		c := k.Cols[i]
		h = fnvByte(h, byte(c.Kind))
		if c.Null {
			h = fnvByte(h, 1)
			continue
		}
		h = fnvByte(h, 0)
		switch c.Kind {
		case KindString:
			s := c.Str
			h = fnvUint64(h, uint64(len(s)))
			// Indexing a Go string reads its original backing bytes.
			for j := 0; j < len(s); j++ {
				h = fnvByte(h, s[j])
			}
		case KindInt:
			h = fnvUint64(h, uint64(c.I64))
		case KindFloat:
			h = fnvUint64(h, c.Bits)
		}
	}
	return h
}

func FormatKey(k Key) string {
	parts := make([]string, k.N)
	for i := 0; i < k.N; i++ {
		parts[i] = k.Cols[i].Display()
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// CompareKey provides the total order used by the streaming sorted path.
func CompareKey(a, b Key) int {
	n := a.N
	if b.N < n {
		n = b.N
	}
	for i := 0; i < n; i++ {
		if c := cmpCol(a.Cols[i], b.Cols[i]); c != 0 {
			return c
		}
	}
	switch {
	case a.N < b.N:
		return -1
	case a.N > b.N:
		return 1
	default:
		return 0
	}
}

func cmpCol(a, b Col) int {
	if a.Null || b.Null {
		switch {
		case a.Null && b.Null:
			return 0
		case a.Null:
			return -1
		default:
			return 1
		}
	}
	if a.Kind != b.Kind {
		if a.Kind < b.Kind {
			return -1
		}
		return 1
	}
	switch a.Kind {
	case KindString:
		// Go's string comparison is byte-wise and does not copy bytes.
		switch {
		case a.Str < b.Str:
			return -1
		case a.Str > b.Str:
			return 1
		default:
			return 0
		}
	case KindInt:
		switch {
		case a.I64 < b.I64:
			return -1
		case a.I64 > b.I64:
			return 1
		default:
			return 0
		}
	case KindFloat:
		x, y := a.Float(), b.Float()
		xNaN, yNaN := math.IsNaN(x), math.IsNaN(y)
		if xNaN || yNaN {
			switch {
			case xNaN && yNaN:
				return 0
			case xNaN:
				return 1
			default:
				return -1
			}
		}
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default: // Includes +0 == -0 after normalization.
			return 0
		}
	default:
		return 0
	}
}
