// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package merge

import "strings"

func contains(s, sub string) bool               { return strings.Contains(s, sub) }
func stringsReplace(s, old, repl string) string { return strings.Replace(s, old, repl, 1) }
