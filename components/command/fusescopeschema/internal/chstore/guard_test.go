// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import "testing"

// The integration tests' guards see a read of the fork's history, and only that.
func TestGuardRegexes(t *testing.T) {
	reads := []string{"SELECT * FROM signoz_logs.schema_migrations_v2", "SELECT * FROM `signoz_logs`.`schema_migrations_v2` WHERE 1", "SELECT a FROM x JOIN signoz_logs.distributed_schema_migrations_v2 USING a", "SELECT * FROM clusterAllReplicas('cluster', 'signoz_logs', 'schema_migrations_v2')"}
	notReads := []string{"SELECT toString(i), formatQuerySingleLine(a[i]) FROM (SELECT ['SELECT \\'schema_migrations_v2\\''] AS a) ARRAY JOIN arrayEnumerate(a) AS i", "CREATE TABLE IF NOT EXISTS `signoz_logs`.schema_migrations_v2 ON CLUSTER `cluster` (x String) ENGINE = Memory"}
	for _, q := range reads {
		if !historyReadRe.MatchString(q) {
			t.Errorf("missed %s", q)
		}
	}
	for _, q := range notReads {
		if historyReadRe.MatchString(q) {
			t.Errorf("flagged %s", q)
		}
	}
	if !historyWriteRe.MatchString("INSERT INTO `fusescope_schema`.`migrations` (id) SELECT 1") || !historyWriteRe.MatchString("ALTER TABLE `fusescope_schema`.`migrations` UPDATE state = 'done' WHERE 1") || historyWriteRe.MatchString("CREATE TABLE IF NOT EXISTS `fusescope_schema`.`migrations` ON CLUSTER c (id UInt32)") {
		t.Error("historyWriteRe")
	}
}
