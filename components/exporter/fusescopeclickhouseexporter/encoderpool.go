// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"math"
	"runtime/debug"
	"sync"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

// defaultIdleBudget bounds the idle encoders of the process without GOMEMLIMIT.
// A traces encoder that held a 20,000-span batch of 15 attributes per span
// keeps about 45 MiB (TestRetainedEstimate).
const (
	defaultMaxIdleEncoders = 4
	defaultIdleBudget      = 128 << 20
)

// processIdleBudget is shared by every exporter of the process: an eighth of
// GOMEMLIMIT when set, so idle encoders stay well under the point where
// memory_limiter refuses data.
var processIdleBudget = newIdleBudget(idleBudgetFor(debug.SetMemoryLimit(-1)))

func idleBudgetFor(memoryLimit int64) int {
	if memoryLimit <= 0 || memoryLimit == math.MaxInt64 {
		return defaultIdleBudget
	}
	return int(memoryLimit / 8)
}

// idleBudget is the memory idle encoders may keep, across pools.
type idleBudget struct {
	mu        sync.Mutex
	used, max int
}

func newIdleBudget(maxBytes int) *idleBudget { return &idleBudget{max: maxBytes} }

func (b *idleBudget) take(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+n > b.max {
		return false
	}
	b.used += n
	return true
}

func (b *idleBudget) give(n int) {
	b.mu.Lock()
	b.used -= n
	b.mu.Unlock()
}

// retainer is an encoder that is kept between exports.
type retainer interface {
	attempter
	// retire empties the encoder and drops its references to the batch it
	// encoded, keeping the capacity of its columns, maps and buffers. It returns
	// an estimate of the bytes of that capacity outside the tables' columns.
	retire() int
}

// encoderPool keeps idle encoders so their columns keep the capacity of earlier
// batches. A sync.Pool loses its items at the second garbage collection after
// they are put back, and under GOMEMLIMIT a busy collector collects about once
// per batch: the columns would regrow from empty every batch. The pool keeps at
// most maxIdle encoders, none estimated over maxEach, within a budget shared by
// the process; an encoder that does not fit, as after an unusually large batch,
// is dropped whole. It is safe for concurrent use.
type encoderPool[E retainer] struct {
	newEncoder func() E
	tables     []table[E]

	maxIdle, maxEach int
	budget           *idleBudget

	mu   sync.Mutex
	idle []idleEncoder[E]
}

type idleEncoder[E any] struct {
	enc   E
	bytes int
}

func newEncoderPool[E retainer](newEncoder func() E, tables []table[E], maxIdle int, budget *idleBudget) *encoderPool[E] {
	if maxIdle < 1 {
		maxIdle = defaultMaxIdleEncoders
	}
	return &encoderPool[E]{
		newEncoder: newEncoder, tables: tables,
		maxIdle: maxIdle, maxEach: budget.max / 2, budget: budget,
	}
}

func (p *encoderPool[E]) get() E {
	p.mu.Lock()
	n := len(p.idle)
	if n == 0 {
		p.mu.Unlock()
		return p.newEncoder()
	}
	e := p.idle[n-1]
	p.idle[n-1] = idleEncoder[E]{}
	p.idle = p.idle[:n-1]
	p.mu.Unlock()
	p.budget.give(e.bytes)
	return e.enc
}

// drain drops every idle encoder and gives its bytes back to the budget, so an
// exporter that shut down, as on a configuration reload, holds none of it.
func (p *encoderPool[E]) drain() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, e := range idle {
		p.budget.give(e.bytes)
	}
}

// put retires enc and keeps it if it fits; otherwise enc is dropped.
func (p *encoderPool[E]) put(enc E) {
	n, ok := p.retire(enc)
	if !ok || n > p.maxEach {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) >= p.maxIdle || !p.budget.take(n) {
		return
	}
	p.idle = append(p.idle, idleEncoder[E]{enc, n})
}

// retire empties enc and estimates the bytes it keeps. ok is false when a
// column cannot be measured, so the encoder is not kept.
func (p *encoderPool[E]) retire(enc E) (n int, ok bool) {
	ok = true
	for _, t := range p.tables {
		for _, col := range t.input(enc) {
			c, known := retireColumn(col.Data)
			n += c
			ok = ok && known
		}
	}
	return n + enc.retire(), ok
}

// retireColumn estimates the bytes a column keeps and clears the strings its
// spare capacity still references, so an idle encoder does not hold the data
// of an old batch. It reports false for a column type it does not know.
func retireColumn(c proto.ColInput) (int, bool) {
	switch c := c.(type) {
	case *proto.ColStr:
		return cap(c.Buf) + cap(c.Pos)*int(unsafe.Sizeof(proto.Position{})), true
	case *proto.ColJSONStr:
		return retireColumn(&c.Str)
	case *proto.ColLowCardinality[string]:
		// Values, plus about as much again for the keys and the dictionary,
		// which the column keeps unexported.
		n := retireSlice(c.Values)
		return 2*n + cap(c.Values)*8, true
	case *proto.ColEnum:
		return retireSlice(c.Values) + cap(c.Values)*2, true
	case *proto.ColArr[string]:
		n, ok := retireColumn(c.Data)
		return n + retireSlice(c.Offsets), ok
	case *proto.ColMap[string, string]:
		return retireMap(&c.Offsets, c.Keys, c.Values)
	case *proto.ColMap[string, float64]:
		return retireMap(&c.Offsets, c.Keys, c.Values)
	case *proto.ColMap[string, bool]:
		return retireMap(&c.Offsets, c.Keys, c.Values)
	case *proto.ColNullable[float64]:
		n, ok := retireColumn(c.Values)
		return n + retireSlice(c.Nulls), ok
	case *proto.ColDateTime64:
		return retireSlice(c.Data), true
	case *proto.ColDateTime:
		return retireSlice(c.Data), true
	case *proto.ColUInt64:
		return retireSlice(*c), true
	case *proto.ColUInt32:
		return retireSlice(*c), true
	case *proto.ColUInt8:
		return retireSlice(*c), true
	case *proto.ColInt64:
		return retireSlice(*c), true
	case *proto.ColInt16:
		return retireSlice(*c), true
	case *proto.ColInt8:
		return retireSlice(*c), true
	case *proto.ColFloat64:
		return retireSlice(*c), true
	case *proto.ColBool:
		return retireSlice(*c), true
	case *proto.ColFixedStr32:
		return retireSlice(*c), true
	}
	return 0, false
}

func retireMap(offsets *proto.ColUInt64, keys, values proto.ColInput) (int, bool) {
	k, okK := retireColumn(keys)
	v, okV := retireColumn(values)
	return retireSlice(*offsets) + k + v, okK && okV
}

// retireSlice clears s up to its capacity and returns the bytes of that capacity.
func retireSlice[T any](s []T) int {
	clear(s[:cap(s)])
	var zero T
	return cap(s) * int(unsafe.Sizeof(zero))
}

// mapBytes estimates the memory of m, which keeps its buckets once cleared:
// each entry at a 7/8 load factor, plus a control byte.
func mapBytes[K comparable, V any](m map[K]V) int {
	var k K
	var v V
	return len(m) * (int(unsafe.Sizeof(k)+unsafe.Sizeof(v)) + 1) * 8 / 7
}
