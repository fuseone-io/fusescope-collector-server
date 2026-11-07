// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// ErrPrecheck: a replica does not meet the supported deployment (§2.1, §7.6).
var ErrPrecheck = errors.New("precheck failed")

// SupportedVersions are the ClickHouse versions in the CI matrix (§9), as
// version() prefixes.
var SupportedVersions = []string{"25.12."}

// MaxClockSkew bounds the difference between the replicas' clocks (§2.1).
const MaxClockSkew = 30 * time.Second

type replicaFacts struct {
	key     string
	version string
	nowMs   int64
}

// evaluatePrecheck refuses a version outside versions or clocks further apart
// than skew.
func evaluatePrecheck(facts []replicaFacts, versions []string, skew time.Duration) error {
	if len(facts) == 0 {
		return fmt.Errorf("%w: no replica answered", ErrPrecheck)
	}
	var errs []error
	minMs, maxMs := facts[0].nowMs, facts[0].nowMs
	for _, f := range facts {
		if !slices.ContainsFunc(versions, func(v string) bool { return strings.HasPrefix(f.version, v) }) {
			errs = append(errs, fmt.Errorf("replica %s runs ClickHouse %s, not in the supported %v", f.key, f.version, versions))
		}
		minMs, maxMs = min(minMs, f.nowMs), max(maxMs, f.nowMs)
	}
	if d := time.Duration(maxMs-minMs) * time.Millisecond; d >= skew {
		errs = append(errs, fmt.Errorf("the replicas' clocks differ by %s, %s at most", d, skew))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrPrecheck, err)
	}
	return nil
}

// Precheck runs the read-only precheck on every replica (§2.1): the cluster
// exists, the version is supported, the clocks agree, the shard and replica
// macros are set, and system.zookeeper is readable.
func (s *Store) Precheck(ctx context.Context) error {
	refuse := func(what string, err error) error {
		if isCode(err, proto.ErrClusterDoesntExist, proto.ErrNoElementsInConfig, proto.ErrNoZookeeper, proto.ErrKeeperException, proto.ErrUnknownTable) {
			return fmt.Errorf("%w: %s: %w", ErrPrecheck, what, err)
		}
		return fmt.Errorf("precheck, %s: %w", what, err)
	}
	if _, err := s.Replicas(ctx); err != nil {
		return refuse("cluster "+s.cluster+" in remote_servers, every replica answering", err)
	}
	rows, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT %s, version(), toString(toUnixTimestamp64Milli(now64(3))) FROM clusterAllReplicas(%s, system.one)", replicaKey, quote(s.cluster)), 3)
	if err != nil {
		return refuse("version and clock", err)
	}
	facts := make([]replicaFacts, len(rows))
	for i, r := range rows {
		ms, err := strconv.ParseInt(r[2], 10, 64)
		if err != nil {
			return err
		}
		facts[i] = replicaFacts{key: r[0], version: r[1], nowMs: ms}
	}
	if err := evaluatePrecheck(facts, SupportedVersions, MaxClockSkew); err != nil {
		return err
	}
	macros, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT %s, getMacro('shard'), getMacro('replica') FROM clusterAllReplicas(%s, system.one)", replicaKey, quote(s.cluster)), 3)
	if err != nil {
		return refuse("the shard and replica macros", err)
	}
	for _, m := range macros {
		if m[1] == "" || m[2] == "" {
			return fmt.Errorf("%w: replica %s has an empty shard or replica macro", ErrPrecheck, m[0])
		}
	}
	if _, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT toString(count()) FROM clusterAllReplicas(%s, system.zookeeper) WHERE path = '/'", quote(s.cluster)), 1); err != nil {
		return refuse("system.zookeeper readable", err)
	}
	return nil
}
