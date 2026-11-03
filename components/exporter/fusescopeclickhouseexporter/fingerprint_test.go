// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The resource JSON column leaves out a key that is also the prefix of another
// key, as the attributes JSON column does; ClickHouse refuses the whole insert
// otherwise ("Duplicate path found during parsing JSON object"). labels and
// resources_string keep both (traces spec §2.2).
func TestResourceJSONLeavesOutShadowedKeys(t *testing.T) {
	tests := []struct {
		name         string
		attrs        [][2]string
		labels, json string
	}{
		{"no collision", [][2]string{{"service.name", "a"}, {"host.name", "h"}},
			`{"host.name":"h","service.name":"a"}`, `{"host.name":"h","service.name":"a"}`},
		{"leaf first", [][2]string{{"deployment.environment", "prod"}, {"deployment.environment.name", "production"}},
			`{"deployment.environment":"prod","deployment.environment.name":"production"}`,
			`{"deployment.environment.name":"production"}`},
		{"leaf last", [][2]string{{"a.b.c", "1"}, {"a-b", "2"}, {"a", "3"}},
			`{"a":"3","a-b":"2","a.b.c":"1"}`, `{"a-b":"2","a.b.c":"1"}`},
		{"not a dotted prefix", [][2]string{{"ab", "1"}, {"a", "2"}}, `{"a":"2","ab":"1"}`, `{"a":"2","ab":"1"}`},
		{"prefix ladder", [][2]string{{"a", "1"}, {"aa", "2"}, {"a-", "3"}, {"a/b", "4"}, {"aa.b", "5"}, {"a.", "6"}},
			`{"a":"1","a-":"3","a.":"6","a/b":"4","aa":"2","aa.b":"5"}`, `{"a-":"3","a.":"6","a/b":"4","aa.b":"5"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := pcommon.NewResource()
			for _, kv := range tt.attrs {
				res.Attributes().PutStr(kv[0], kv[1])
			}
			info, _ := newResourceInfo(res, nil)
			if info.labels != tt.labels {
				t.Errorf("labels %s, want %s", info.labels, tt.labels)
			}
			if info.json != tt.json {
				t.Errorf("resource JSON %s, want %s", info.json, tt.json)
			}

			ld := plog.NewLogs()
			rl := ld.ResourceLogs().AppendEmpty()
			res.CopyTo(rl.Resource())
			rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("r")
			logs := newLogsEncoder()
			if err := logs.encode(ld, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			if got := firstJSON(logs.rows.resource); got != tt.json {
				t.Errorf("logs resource column %s, want %s", got, tt.json)
			}

			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			res.CopyTo(rs.Resource())
			s := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			s.SetTraceID(pcommon.TraceID{1})
			s.SetSpanID(pcommon.SpanID{2})
			traces := newTracesEncoder()
			if err := traces.encode(td, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			if got := firstJSON(traces.index.resource); got != tt.json {
				t.Errorf("traces resource column %s, want %s", got, tt.json)
			}
		})
	}
}

func TestCompareDotted(t *testing.T) {
	tests := []struct {
		s, key string
		want   int
	}{
		{"a.", "a", 0}, {"a", "a", -1}, {"a.b", "a", 1}, {"a-b", "a", -1}, {"a/", "a", 1},
		{"", "a", -1}, {"b", "a", 1}, {"aa", "ab", -1}, {"a.", "", 1}, {".", "", 0},
	}
	for _, tt := range tests {
		if got := compareDotted(tt.s, tt.key); got != tt.want {
			t.Errorf("compareDotted(%q, %q) = %d, want %d", tt.s, tt.key, got, tt.want)
		}
	}
}

func BenchmarkResourceInfo(b *testing.B) {
	for _, name := range []string{"plain", "shadowed", "prefix ladder"} {
		res := pcommon.NewResource()
		for _, k := range []string{"service.name", "service.namespace", "deployment.environment", "host.name",
			"k8s.pod.name", "k8s.namespace.name", "telemetry.sdk.language", "process.pid"} {
			res.Attributes().PutStr(k, "value")
		}
		switch name {
		case "shadowed":
			res.Attributes().PutStr("deployment.environment.name", "value")
		case "prefix ladder": // a, aa, aaa, …: every key a prefix of the later ones, none dotted
			for i := 1; i <= 2048; i++ {
				res.Attributes().PutStr(strings.Repeat("a", i), "v")
			}
		}
		b.Run(name, func(b *testing.B) {
			var buf []byte
			b.ReportAllocs()
			for b.Loop() {
				_, buf = newResourceInfo(res, buf)
			}
		})
	}
}

func firstJSON(c proto.ColJSONStr) string {
	var first string
	_ = c.ForEach(func(i int, s string) error {
		if i == 0 {
			first = s
		}
		return nil
	})
	return first
}
