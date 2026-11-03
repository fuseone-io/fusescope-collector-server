// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

//go:build race

package rumreceiver

// The race detector disables sync.Pool reuse, so each request allocates a
// fresh body buffer that a pooled run does not.
const raceEnabled = true
