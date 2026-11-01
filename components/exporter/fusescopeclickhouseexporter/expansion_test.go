// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Every log record and span copies its resource, so a large resource over many
// small records expands far beyond the request: the rows are inserted in
// chunks, without losing any, and cut the same way on every attempt.
func TestLogsAndTracesChunks(t *testing.T) {
	big := strings.Repeat("x", 64<<10)
	logs := func() plog.Logs {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("big", big)
		records := rl.ScopeLogs().AppendEmpty().LogRecords()
		for range 100 {
			records.AppendEmpty().Body().SetStr("r")
		}
		return ld
	}()
	traces := func() ptrace.Traces {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("big", big)
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		for i := range 100 {
			s := spans.AppendEmpty()
			s.SetTraceID(pcommon.TraceID{1, byte(i)})
			s.SetSpanID(pcommon.SpanID{2, byte(i)})
		}
		return td
	}()
	tests := []struct {
		name   string
		encode func(chunkBytes int) []int // rows per chunk
	}{
		{"logs", func(chunkBytes int) []int {
			enc := newLogsEncoder()
			enc.maxChunkBytes = chunkBytes
			var rows []int
			if err := enc.encode(logs, func() error { rows = append(rows, enc.rows.id.Rows()); return nil }); err != nil {
				t.Fatal(err)
			}
			return append(rows, enc.rows.id.Rows())
		}},
		{"traces", func(chunkBytes int) []int {
			enc := newTracesEncoder()
			enc.maxChunkBytes = chunkBytes
			var rows []int
			if err := enc.encode(traces, func() error { rows = append(rows, enc.index.spanID.Rows()); return nil }); err != nil {
				t.Fatal(err)
			}
			return append(rows, enc.index.spanID.Rows())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if whole := tt.encode(defaultChunkBytes); !slices.Equal(whole, []int{100}) {
				t.Fatalf("one chunk holds %v rows, want [100]", whole)
			}
			split := tt.encode(1 << 20)
			total := 0
			for _, n := range split {
				total += n
				if n > 10 {
					t.Errorf("a chunk holds %d rows of a 64 KiB resource, want at most 10 under 1 MiB", n)
				}
			}
			if total != 100 || len(split) < 10 {
				t.Errorf("chunks %v, want 100 rows in at least 10", split)
			}
			if again := tt.encode(1 << 20); !slices.Equal(again, split) {
				t.Errorf("chunks %v, then %v: a retry would cut them differently", split, again)
			}
		})
	}
}
