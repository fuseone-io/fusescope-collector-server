// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"regexp"
	"slices"
	"strings"
)

// The schema changes the backend makes at runtime (§8). Each pattern builds
// the text the backend writes and requires the live text to be its canonical
// form; anything else is drift. Sources are in the FuseScope backend [be]:
//   - promoted columns, _exists and _idx/_minmax_idx:
//     pkg/query-service/app/clickhouseReader/reader.go (UpdateLogField,
//     UpdateTraceField), pkg/query-service/utils/format.go
//     (GetClickhouseColumnNameV2), and the RUM promoter
//     pkg/modules/rumapplication/implrumapplication/promoter.go;
//   - JSON subcolumn indexes: pkg/types/promotetypes/target.go and types.go,
//     pkg/telemetryschema/chschema/index.go,
//     pkg/telemetryschema/tracestelemetryschema/storage.go
//     (AttributeJSONValueExpr);
//   - retention: reader.go (the custom logs retention) and
//     ttl_statements.go, pkg/types/retentiontypes/ttlclause.go.

// promotedTables hold promoted columns; the local ones also their indexes.
var (
	promotedTables      = []string{"logs.logs_v2", "logs.distributed_logs_v2", "traces.signoz_index_v3", "traces.distributed_signoz_index_v3"}
	promotedLocalTables = []string{"logs.logs_v2", "traces.signoz_index_v3"}
)

var promotedRe = regexp.MustCompile(`^(attribute|resource)_(string|bool|number)_(.+)$`)

// promotedTypes are each data type's column type (constants.ChDataTypeMap).
var promotedTypes = map[string]string{"string": "String", "bool": "Bool", "number": "Float64"}

const promotedCodec = "CODEC(ZSTD(1))"

// promoted splits a promoted column's name into the map it reads, the key and
// its data type.
func promoted(name string) (mapColumn, key, dataType string, ok bool) {
	m := promotedRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", "", false
	}
	return m[1] + "s_" + m[2], strings.ReplaceAll(m[3], "$$", "."), m[2], true
}

// stringLiteral quotes s as the backend's fmt "'%s'" and StringLiteral agree
// on for the keys these patterns read.
func stringLiteral(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

func exprCheck(want, got string) check { return check{want: want, got: got} }

func hasColumn(o Object, name string) (Column, bool) {
	i := slices.IndexFunc(o.Columns, func(c Column) bool { return c.Name == name })
	if i < 0 {
		return Column{}, false
	}
	return o.Columns[i], true
}

// allowedColumn returns the alternatives under which an extra column is a
// promoted column or its _exists companion.
func allowedColumn(table string, c Column, live Object) [][]check {
	if !slices.Contains(promotedTables, table) || c.DefaultKind != "DEFAULT" || c.Codec != promotedCodec || c.Comment != "" {
		return nil
	}
	var alts [][]check
	if mapColumn, key, dataType, ok := promoted(c.Name); ok && c.Type == promotedTypes[dataType] {
		alts = append(alts, []check{exprCheck(mapColumn+"["+stringLiteral(key)+"]", c.DefaultExpression)})
	}
	if base, ok := strings.CutSuffix(c.Name, "_exists"); ok && c.Type == "Bool" {
		if mapColumn, key, _, ok := promoted(base); ok {
			if _, present := hasColumn(live, base); present {
				alts = append(alts, []check{exprCheck("if(mapContains("+mapColumn+", "+stringLiteral(key)+") != 0, true, false)", c.DefaultExpression)})
			}
		}
	}
	return alts
}

// retentionTables are the tables whose _retention_days and
// _retention_days_cold DEFAULT the custom logs retention rewrites.
var retentionTables = []string{"logs.logs_v2", "logs.distributed_logs_v2", "logs.logs_v2_resource", "logs.distributed_logs_v2_resource"}

// allowedColumnChange allows a new DEFAULT value of a retention column, its
// type and everything else unchanged.
func allowedColumnChange(table string, want, got Column) [][]check {
	if !slices.Contains(retentionTables, table) || want.Name != "_retention_days" && want.Name != "_retention_days_cold" {
		return nil
	}
	same := want
	same.DefaultExpression = got.DefaultExpression
	if got != same || want.Type != "UInt16" || got.DefaultKind != "DEFAULT" || got.DefaultExpression == "" {
		return nil
	}
	return [][]check{{}}
}

// indexTypeRe is the index types the backend accepts (promotetypes'
// indexTypeRe), each with its base type.
var indexTypeRe = regexp.MustCompile(`^(?:minmax|set\(\s*(\d{1,7})\s*\)|bloom_filter(?:\(\s*(\d+(?:\.\d+)?|\.\d+)\s*\))?|tokenbf_v1\(\s*(\d{1,7})\s*,\s*(\d{1,2})\s*,\s*(\d{1,10})\s*\)|ngrambf_v1\(\s*(\d{1,2})\s*,\s*(\d{1,7})\s*,\s*(\d{1,2})\s*,\s*(\d{1,10})\s*\))$`)

var indexBases = []string{"ngrambf_v1", "tokenbf_v1", "minmax", "bloom_filter", "set"}

func indexBase(typ string) string {
	if !indexTypeRe.MatchString(typ) {
		return ""
	}
	for _, b := range indexBases {
		if strings.HasPrefix(typ, b) {
			return b
		}
	}
	return ""
}

// jsonTarget is a JSON column the promote module indexes subcolumns of.
type jsonTarget struct {
	columns []string // the base column and the promoted one
	logs    bool
	// dataTypes are the JSON data types an index may read, as the name spells
	// them; traces fold Int64 into Float64.
	dataTypes map[string]string
}

var jsonTargets = map[string]jsonTarget{
	"logs.logs_v2": {columns: []string{"body_v2", "body_promoted"}, logs: true,
		dataTypes: map[string]string{"string": "String", "int64": "Int64", "float64": "Float64"}},
	"traces.signoz_index_v3": {columns: []string{"attributes", "attributes_promoted"},
		dataTypes: map[string]string{"string": "String", "float64": "Float64"}},
}

// allowedIndex returns the alternatives under which an extra index is a
// promoted column's _idx or _minmax_idx, or a JSON subcolumn index.
func allowedIndex(table string, i Index, live Object) [][]check {
	var alts [][]check
	if slices.Contains(promotedLocalTables, table) {
		for _, suffix := range []string{"_idx", "_minmax_idx"} {
			col, ok := strings.CutSuffix(i.Name, suffix)
			if !ok {
				continue
			}
			c, present := hasColumn(live, col)
			_, _, dataType, isPromoted := promoted(col)
			if !present || !isPromoted || len(allowedColumn(table, c, live)) == 0 {
				continue
			}
			switch {
			case suffix == "_idx" && dataType != "bool" && indexBase(i.Type) != "" && i.Granularity > 0,
				suffix == "_minmax_idx" && dataType == "number" && i.Type == "minmax" && i.Granularity == 1:
				alts = append(alts, []check{exprCheck(identifier(col), i.Expression)})
			}
		}
	}
	if target, ok := jsonTargets[table]; ok && i.Granularity > 0 {
		if want, ok := target.indexExpression(i); ok {
			alts = append(alts, []check{exprCheck(want, i.Expression)})
		}
	}
	return alts
}

// indexExpression parses a JSON subcolumn index name,
// "<column>.<path>_<jsontype>_<indextype>", and builds the expression the
// promote module writes for it.
func (t jsonTarget) indexExpression(i Index) (string, bool) {
	base := indexBase(i.Type)
	rest, ok := strings.CutSuffix(i.Name, "_"+base)
	if base == "" || !ok {
		return "", false
	}
	for name, dataType := range t.dataTypes {
		withPath, ok := strings.CutSuffix(rest, "_"+name)
		if !ok {
			continue
		}
		for _, column := range t.columns {
			path, ok := strings.CutPrefix(withPath, column+".")
			if !ok || path == "" {
				continue
			}
			if t.logs {
				expr := "assumeNotNull(dynamicElement(" + jsonPathSegment(column) + "." + jsonPath(path) + ", " + stringLiteral(dataType) + "))"
				if dataType == "String" {
					expr = "lower(" + expr + ")"
				}
				return expr, true
			}
			sub := column + "." + identifier(path)
			if dataType == "String" {
				return sub + "::String", true
			}
			return "toFloat64OrNull(" + sub + "::String)", true
		}
	}
	return "", false
}

// jsonPath and jsonPathSegment spell a logs JSON path as chschema does:
// a segment holding '-', ':' or '@' is backquoted, any other kept bare.
func jsonPath(path string) string {
	segments := strings.Split(path, ".")
	for i, s := range segments {
		segments[i] = jsonPathSegment(s)
	}
	return strings.Join(segments, ".")
}

func jsonPathSegment(s string) string {
	if strings.ContainsAny(s, "-:@") {
		return "`" + s + "`"
	}
	return s
}

// identifier quotes as clickhousesql.Identifier: backquotes, with '\' and '`'
// escaped, and '$' before '{', '?' or a digit written \x24.
func identifier(s string) string {
	var b strings.Builder
	b.WriteByte('`')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' || c == '`':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '$' && i+1 < len(s) && (s[i+1] == '{' || s[i+1] == '?' || s[i+1] >= '0' && s[i+1] <= '9'):
			b.WriteString(`\x24`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('`')
	return b.String()
}

// The sentinels stand for the numbers and the volume a retention setting
// chooses; they survive canonicalisation unchanged.
const (
	sentinelDelete = "111111111"
	sentinelMove   = "222222222"
	sentinelCold   = "333333333"
	sentinelVolume = "'zzvolumezz'"
)

// ttlShape is how the retention settings write one table's TTL
// (TTLClause.Statement): the time, then each delete and move interval form.
type ttlShape struct {
	time   string
	delete []string
	move   []string
}

var (
	seconds    = func(s string) string { return "INTERVAL " + s + " SECOND" }
	logsDelete = []string{seconds(sentinelDelete), "toIntervalDay(_retention_days)"}
	logsMove   = []string{seconds(sentinelMove), "toIntervalDay(if(_retention_days_cold = 0, " + sentinelCold + ", _retention_days_cold))"}
	keysDelete = []string{seconds(sentinelDelete), "toIntervalDay(" + sentinelDelete + ")"}
	timeTraces = "toDateTime(timestamp)"
	timeMetric = "toDateTime(toUInt32(unix_milli / 1000), 'UTC')"
	bucket     = "toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800)"
	tracesTTL  = func(time string) ttlShape {
		return ttlShape{time: time, delete: []string{seconds(sentinelDelete)}, move: []string{seconds(sentinelMove)}}
	}
	metricsTTL = tracesTTL(timeMetric)
)

var ttlShapes = map[string]ttlShape{
	"logs.logs_v2":                       {time: "toDateTime(timestamp / 1000000000)", delete: logsDelete, move: logsMove},
	"logs.logs_v2_resource":              {time: bucket, delete: logsDelete, move: logsMove},
	"logs.logs_attribute_keys":           {time: "timestamp", delete: keysDelete},
	"logs.logs_resource_keys":            {time: "timestamp", delete: keysDelete},
	"traces.signoz_index_v3":             tracesTTL(timeTraces),
	"traces.traces_v3_resource":          tracesTTL(bucket),
	"traces.signoz_error_index_v2":       tracesTTL(timeTraces),
	"traces.usage_explorer":              tracesTTL(timeTraces),
	"traces.dependency_graph_minutes_v2": tracesTTL(timeTraces),
	"traces.trace_summary":               tracesTTL("toDateTime(end)"),
	"traces.span_attributes_keys":        {time: timeTraces, delete: []string{seconds(sentinelDelete)}},
	"metrics.samples_v4":                 metricsTTL,
	"metrics.samples_v4_agg_5m":          metricsTTL,
	"metrics.samples_v4_agg_30m":         metricsTTL,
	"metrics.exp_hist":                   metricsTTL,
	"metrics.time_series_v4":             metricsTTL,
	"metrics.time_series_v4_6hrs":        metricsTTL,
	"metrics.time_series_v4_1day":        metricsTTL,
	"metrics.time_series_v4_1week":       metricsTTL,
}

// allowedTTL returns one alternative per TTL the table's retention settings
// can write.
func allowedTTL(table, got string) [][]check {
	shape, ok := ttlShapes[table]
	if !ok {
		return nil
	}
	var alts [][]check
	for _, d := range shape.delete {
		rule := shape.time + " + " + d + " DELETE"
		alts = append(alts, []check{{ttl: true, want: rule, got: got}})
		for _, m := range shape.move {
			alts = append(alts, []check{{ttl: true, want: rule + ", " + shape.time + " + " + m + " TO VOLUME " + sentinelVolume, got: got}})
		}
	}
	return alts
}

// ttlPattern turns a canonical TTL template into an anchored pattern where
// each sentinel matches any number or volume.
func ttlPattern(canonical string) *regexp.Regexp {
	p := regexp.QuoteMeta(canonical)
	for _, n := range []string{sentinelDelete, sentinelMove, sentinelCold} {
		p = strings.ReplaceAll(p, n, `\d+`)
	}
	p = strings.ReplaceAll(p, regexp.QuoteMeta(sentinelVolume), `'(?:[^'\\]|\\.)+'`)
	return regexp.MustCompile("^" + p + "$")
}
