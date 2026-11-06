// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package schema compares the telemetry schema as structures read from the
// system tables of every replica (docs/spec/fusescope-schema.md §4.1), with
// the changes the backend makes at runtime allowed by pattern (§8).
package schema

import (
	"slices"
	"strings"
)

// Roles are the six telemetry databases, by the role their name is replaced
// with, so custom names compare (§2, §4.1).
var Roles = []string{"analytics", "logs", "metadata", "meter", "metrics", "traces"}

// DefaultDatabases are the chart's database names.
var DefaultDatabases = map[string]string{
	"analytics": "signoz_analytics",
	"logs":      "signoz_logs",
	"metadata":  "signoz_metadata",
	"meter":     "signoz_meter",
	"metrics":   "signoz_metrics",
	"traces":    "signoz_traces",
}

// ClusterPlaceholder replaces the cluster name; RolePlaceholder a database name.
const ClusterPlaceholder = "{cluster}"

func RolePlaceholder(role string) string { return "{db:" + role + "}" }

// Kind is what an object is.
type Kind string

const (
	KindTable            Kind = "table"
	KindDistributed      Kind = "distributed"
	KindMaterializedView Kind = "materialized_view"
)

// Snapshot is one replica's telemetry schema, keyed by "<role>.<name>". Every
// expression in it is canonicalised by the server with formatQuery, and
// database and cluster names are replaced by their placeholders.
type Snapshot map[string]Object

// Object is one table, distributed table or materialized view, with what §4.1
// compares.
type Object struct {
	Kind Kind `json:"kind"`
	// Engine and EngineArgs come from engine_full. For a Distributed table the
	// arguments are the cluster, database, table and sharding key.
	Engine       string   `json:"engine,omitempty"`
	EngineArgs   []string `json:"engine_args,omitempty"`
	SortingKey   string   `json:"sorting_key,omitempty"`
	PrimaryKey   string   `json:"primary_key,omitempty"`
	PartitionKey string   `json:"partition_key,omitempty"`
	SamplingKey  string   `json:"sampling_key,omitempty"`
	TTL          string   `json:"ttl,omitempty"`
	Settings     []string `json:"settings,omitempty"`
	// Columns are in position order.
	Columns     []Column     `json:"columns,omitempty"`
	Indexes     []Index      `json:"indexes,omitempty"`
	Projections []Projection `json:"projections,omitempty"`
	// Target, Refresh and Query describe a materialized view; Refresh is the
	// REFRESH clause of a refreshable one, as the server prints it.
	Target  string `json:"target,omitempty"`
	Refresh string `json:"refresh,omitempty"`
	Query   string `json:"query,omitempty"`
}

type Column struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	DefaultKind       string `json:"default_kind,omitempty"`
	DefaultExpression string `json:"default_expression,omitempty"`
	Codec             string `json:"codec,omitempty"`
	Comment           string `json:"comment,omitempty"`
}

// Index is a data-skipping index; Type is type_full, the type with its
// arguments.
type Index struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Expression  string `json:"expression"`
	Granularity uint64 `json:"granularity"`
}

type Projection struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// Names returns a snapshot's object keys, sorted.
func (s Snapshot) Names() []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Split returns an object key's role and name.
func Split(key string) (role, name string) {
	role, name, _ = strings.Cut(key, ".")
	return role, name
}
