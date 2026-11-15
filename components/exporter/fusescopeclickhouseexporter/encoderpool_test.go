// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadatatest"
)

const testBudget = 1 << 30

func tracesPool(maxIdle int) *encoderPool[*tracesEncoder] {
	return newEncoderPool(newTracesEncoder, tracesTables, maxIdle, newIdleBudget(testBudget))
}

func encodedTraces(spans int) *tracesEncoder {
	enc := newTracesEncoder()
	_ = enc.encode(syntheticTraces(spans), nil)
	return enc
}

// Two collections in a row empty a sync.Pool (the first moves its items to the
// victim cache, the second drops them); the pool must keep its encoders.
func TestEncoderPoolSurvivesTwoCollections(t *testing.T) {
	p := tracesPool(4)
	enc := encodedTraces(100)
	p.put(enc)
	runtime.GC()
	runtime.GC()
	if got := p.get(); got != enc {
		t.Fatal("the encoder put back was not reused after two collections")
	}
}

// A reused encoder holds no rows and no references to the batch it encoded.
func TestRetiredEncodersHoldNoRowsOrReferences(t *testing.T) {
	check := func(t *testing.T, inputs []proto.Input) {
		t.Helper()
		for _, in := range inputs {
			for _, col := range in {
				if n := col.Data.Rows(); n != 0 {
					t.Errorf("column %s keeps %d rows", col.Name, n)
				}
				assertNoStrings(t, col.Name, col.Data)
			}
		}
	}
	t.Run("traces", func(t *testing.T) {
		p := tracesPool(1)
		enc := encodedTraces(1_000)
		p.put(enc)
		check(t, inputsOf(p.get(), tracesTables))
		if len(enc.kvs) != 0 || len(enc.resInfos) != 0 || len(enc.seenKeys) != 0 || enc.emit != nil {
			t.Error("auxiliary state survives retire")
		}
		for _, kv := range enc.kvs[:cap(enc.kvs)] {
			if kv != (keyValue{}) {
				t.Fatal("kvs keeps a key or value of the old batch")
			}
		}
		// The next batch has exactly its own rows.
		_ = enc.encode(syntheticTraces(10), nil)
		for _, col := range enc.index.input() {
			if n := col.Data.Rows(); n != 10 {
				t.Fatalf("column %s has %d rows, want 10", col.Name, n)
			}
		}
	})
	t.Run("logs", func(t *testing.T) {
		p := newEncoderPool(newLogsEncoder, logsTables, 1, newIdleBudget(testBudget))
		enc := newLogsEncoder()
		_ = enc.encode(syntheticLogs(), nil)
		p.put(enc)
		check(t, inputsOf(p.get(), logsTables))
		for _, kv := range enc.scope[:cap(enc.scope)] {
			if kv != (keyValue{}) {
				t.Fatal("scope keeps a key or value of the old batch")
			}
		}
	})
	t.Run("metrics", func(t *testing.T) {
		p := newEncoderPool(newMetricsEncoder, metricsTables, 1, newIdleBudget(testBudget))
		enc := newMetricsEncoder()
		_ = enc.encode(syntheticMetrics(1_000), nil)
		p.put(enc)
		check(t, inputsOf(p.get(), metricsTables))
		if len(enc.seenSeries) != 0 || len(enc.metaIndex) != 0 || len(enc.metaRows) != 0 {
			t.Error("metrics maps survive retire")
		}
		for _, kv := range enc.resource[:cap(enc.resource)] {
			if kv != (typedKV{}) {
				t.Fatal("resource keeps a key or value of the old batch")
			}
		}
	})
	t.Run("meter", func(t *testing.T) {
		p := newEncoderPool(newMeterEncoder, meterTables, 1, newIdleBudget(testBudget))
		enc := newMeterEncoder()
		_ = enc.encode(syntheticMeter(), nil)
		p.put(enc)
		check(t, inputsOf(p.get(), meterTables))
	})
}

// Every column of every table must be measured, or no encoder is ever kept.
func TestEveryColumnIsMeasured(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
	}{}
	{
		enc := encodedTraces(100)
		_, ok := tracesPool(1).retire(enc)
		cases = append(cases, struct {
			name string
			ok   bool
		}{"traces", ok})
	}
	{
		enc := newLogsEncoder()
		_ = enc.encode(syntheticLogs(), nil)
		_, ok := newEncoderPool(newLogsEncoder, logsTables, 1, newIdleBudget(testBudget)).retire(enc)
		cases = append(cases, struct {
			name string
			ok   bool
		}{"logs", ok})
	}
	{
		enc := newMetricsEncoder()
		_ = enc.encode(syntheticMetrics(1_000), nil)
		_, ok := newEncoderPool(newMetricsEncoder, metricsTables, 1, newIdleBudget(testBudget)).retire(enc)
		cases = append(cases, struct {
			name string
			ok   bool
		}{"metrics", ok})
	}
	{
		enc := newMeterEncoder()
		_ = enc.encode(syntheticMeter(), nil)
		_, ok := newEncoderPool(newMeterEncoder, meterTables, 1, newIdleBudget(testBudget)).retire(enc)
		cases = append(cases, struct {
			name string
			ok   bool
		}{"meter", ok})
	}
	for _, c := range cases {
		if !c.ok {
			t.Errorf("%s: a column type is not measured", c.name)
		}
	}
	if _, ok := retireColumn(new(proto.ColIPv4)); ok {
		t.Error("an unknown column type was reported as measured")
	}
}

func TestEncoderPoolLimits(t *testing.T) {
	tests := []struct {
		name            string
		maxIdle, puts   int
		budget, maxEach int
		wantIdle        int
	}{
		{name: "keeps up to maxIdle", maxIdle: 2, puts: 3, budget: testBudget, maxEach: testBudget, wantIdle: 2},
		{name: "drops an encoder over maxEach", maxIdle: 4, puts: 1, budget: testBudget, maxEach: 1, wantIdle: 0},
		{name: "drops what does not fit the budget", maxIdle: 4, puts: 3, budget: 0, maxEach: testBudget, wantIdle: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newEncoderPool(newTracesEncoder, tracesTables, tt.maxIdle, newIdleBudget(tt.budget))
			p.maxEach = tt.maxEach
			for range tt.puts {
				p.put(encodedTraces(100))
			}
			if len(p.idle) != tt.wantIdle {
				t.Fatalf("idle = %d, want %d", len(p.idle), tt.wantIdle)
			}
			assertAccounted(t, p.budget, p)
		})
	}
}

// Regression: at 1Gi a 20,000-span encoder (about 67 MiB estimated) was over
// half the 102 MiB budget and dropped after every batch. One encoder as large
// as the whole budget is kept.
func TestEncoderPoolKeepsOneEncoderUpToTheBudget(t *testing.T) {
	enc := encodedTraces(1_000)
	n, _ := tracesPool(1).retire(enc)
	p := newEncoderPool(newTracesEncoder, tracesTables, 4, newIdleBudget(n+n/4))
	p.put(encodedTraces(1_000))
	if len(p.idle) != 1 {
		t.Fatalf("an encoder of %d bytes within a budget of %d was dropped", n, p.budget.max)
	}
	p.put(encodedTraces(1_000))
	if len(p.idle) != 1 || p.budget.used > p.budget.max {
		t.Fatalf("idle %d holding %d of %d bytes, want 1 within the budget", len(p.idle), p.budget.used, p.budget.max)
	}
}

// The budget is shared: a second large encoder, from another pool, does not fit
// beside the first, and get gives its bytes back.
func TestEncoderPoolSharedBudget(t *testing.T) {
	n, _ := tracesPool(1).retire(encodedTraces(1_000))
	budget := newIdleBudget(n + n/2)
	a := newEncoderPool(newTracesEncoder, tracesTables, 4, budget)
	b := newEncoderPool(newTracesEncoder, tracesTables, 4, budget)
	a.maxEach, b.maxEach = n, n
	a.put(encodedTraces(1_000))
	b.put(encodedTraces(1_000))
	if len(a.idle) != 1 || len(b.idle) != 0 || budget.used > budget.max {
		t.Fatalf("idle %d and %d holding %d of %d bytes, want 1 and 0", len(a.idle), len(b.idle), budget.used, budget.max)
	}
	_ = a.get()
	if budget.used != 0 {
		t.Fatalf("after get the budget still holds %d bytes", budget.used)
	}
}

func TestIdleBudgetFollowsGOMEMLIMIT(t *testing.T) {
	tests := []struct {
		limit int64
		want  int
	}{
		{limit: 819 << 20, want: 819 << 20 / 8},
		{limit: math.MaxInt64, want: defaultIdleBudget},
		{limit: 0, want: defaultIdleBudget},
	}
	for _, tt := range tests {
		if got := idleBudgetFor(tt.limit); got != tt.want {
			t.Errorf("idleBudgetFor(%d) = %d, want %d", tt.limit, got, tt.want)
		}
	}
}

// A shut-down exporter gives its idle encoders' budget back.
func TestShutdownReleasesTheBudget(t *testing.T) {
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	x, err := newTracesExporter(createDefaultConfig(), metadatatest.NewSettings(tel))
	if err != nil {
		t.Fatal(err)
	}
	budget := newIdleBudget(testBudget)
	x.encoders.budget = budget
	x.do = func(context.Context, ch.Query) error { return nil }
	if err := x.export(context.Background(), syntheticTraces(100), newRequestID(time.Now())); err != nil {
		t.Fatal(err)
	}
	if budget.used == 0 {
		t.Fatal("no encoder was kept")
	}
	_ = x.shutdown(context.Background())
	if budget.used != 0 || len(x.encoders.idle) != 0 {
		t.Fatalf("after shutdown: %d bytes, %d idle encoders", budget.used, len(x.encoders.idle))
	}
}

func assertAccounted[E retainer](t *testing.T, budget *idleBudget, pools ...*encoderPool[E]) {
	t.Helper()
	sum := 0
	for _, p := range pools {
		if len(p.idle) > p.maxIdle {
			t.Fatalf("idle %d over maxIdle %d", len(p.idle), p.maxIdle)
		}
		for _, e := range p.idle {
			sum += e.bytes
		}
	}
	if budget.used != sum || budget.used > budget.max {
		t.Fatalf("budget used %d, idle sum %d, max %d", budget.used, sum, budget.max)
	}
}

// The pool's limits hold under concurrent exports (run with -race).
func TestEncoderPoolConcurrent(t *testing.T) {
	p := tracesPool(3)
	n, _ := p.retire(encodedTraces(200))
	p.budget.max = 2 * n
	td := syntheticTraces(200)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 20 {
				enc := p.get()
				_ = enc.encode(td, nil)
				if n := enc.index.input()[0].Data.Rows(); n != 200 {
					t.Errorf("rows = %d, want 200", n)
				}
				p.put(enc)
			}
		})
	}
	wg.Wait()
	assertAccounted(t, p.budget, p)
}

// The estimate tracks what an idle encoder really keeps on the heap.
func TestRetainedEstimate(t *testing.T) {
	p := tracesPool(1)
	heap := func() int64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int64(m.HeapAlloc)
	}
	before := heap()
	enc := newTracesEncoder()
	td := syntheticTraces(20_000)
	_ = enc.encode(td, nil)
	td = syntheticTraces(0)
	est, ok := p.retire(enc)
	kept := heap() - before
	runtime.KeepAlive(enc)
	runtime.KeepAlive(td)
	t.Logf("20,000 spans: estimate %d MiB, heap kept %d MiB", est>>20, kept>>20)
	if !ok {
		t.Fatal("not measured")
	}
	if float64(est) < 0.5*float64(kept) || float64(est) > 2*float64(kept) {
		t.Fatalf("estimate %d is not within half to twice the %d bytes kept", est, kept)
	}
}

// The export path reuses one encoder across requests even with collections in
// between.
func TestExportReusesEncoderAcrossCollections(t *testing.T) {
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	x, err := newTracesExporter(createDefaultConfig(), metadatatest.NewSettings(tel))
	if err != nil {
		t.Fatal(err)
	}
	x.do = func(context.Context, ch.Query) error { return nil }
	built := 0
	next := x.encoders.newEncoder
	x.encoders.newEncoder = func() *tracesEncoder { built++; return next() }
	td := syntheticTraces(100)
	for range 3 {
		if err := x.export(context.Background(), td, newRequestID(time.Now())); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		runtime.GC()
	}
	if built != 1 {
		t.Fatalf("built %d encoders for 3 sequential exports, want 1", built)
	}
}

func inputsOf[E any](enc E, tables []table[E]) []proto.Input {
	var out []proto.Input
	for _, t := range tables {
		out = append(out, t.input(enc))
	}
	return out
}

// assertNoStrings fails if a column's spare capacity references a string.
func assertNoStrings(t *testing.T, name string, c proto.ColInput) {
	t.Helper()
	switch c := c.(type) {
	case *proto.ColLowCardinality[string]:
		for _, v := range c.Values[:cap(c.Values)] {
			if v != "" {
				t.Fatalf("column %s keeps the string %q", name, v)
			}
		}
	case *proto.ColEnum:
		for _, v := range c.Values[:cap(c.Values)] {
			if v != "" {
				t.Fatalf("column %s keeps the string %q", name, v)
			}
		}
	case *proto.ColMap[string, string]:
		assertNoStrings(t, name, c.Keys)
		assertNoStrings(t, name, c.Values)
	case *proto.ColMap[string, float64]:
		assertNoStrings(t, name, c.Keys)
	case *proto.ColMap[string, bool]:
		assertNoStrings(t, name, c.Keys)
	case *proto.ColArr[string]:
		assertNoStrings(t, name, c.Data)
	}
}

func syntheticMeter() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("signoz.meter.span.count")
	sum := m.SetEmptySum()
	for i := range 10 {
		p := sum.DataPoints().AppendEmpty()
		p.SetTimestamp(1_793_483_525_000_000_000)
		p.SetIntValue(int64(i))
		p.Attributes().PutStr("service.name", "svc")
	}
	return md
}
