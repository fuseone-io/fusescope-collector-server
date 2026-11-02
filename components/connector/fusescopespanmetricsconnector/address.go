// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector"

import (
	"net/url"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// externalAddress is the address of an external call (apm-metrics §4), or "".
func externalAddress(attrs pcommon.Map) string {
	if host, ok := firstString(attrs, "http.host"); ok && host != "" {
		return host
	}
	if host, ok := firstString(attrs, "server.address", "net.peer.name"); ok && host != "" {
		// With a port attribute the reference writes the host and an empty port.
		if _, port := firstString(attrs, "server.port", "net.peer.port"); port {
			return host + ":"
		}
		return host
	}
	for _, key := range []string{"url.full", "http.url"} {
		if raw, ok := firstString(attrs, key); ok {
			if u, err := url.Parse(raw); err == nil && u.Host != "" {
				return u.Host
			}
		}
	}
	return ""
}
