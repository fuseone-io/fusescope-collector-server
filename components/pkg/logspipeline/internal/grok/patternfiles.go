// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package grok

import "embed"

// PatternFiles are upstream's pattern files, embedded so the binary carries
// them with their license. They are not loaded by default: the default set is
// the map in patterns.go, as upstream's NewWithConfig uses it, and loading more
// would accept pattern names the reference does not know.
//
//go:embed patterns
var PatternFiles embed.FS
