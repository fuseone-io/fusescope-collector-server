// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector"

import (
	"net/url"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// hostPorts are the host keys of an external call, in precedence order, each
// with the port key paired with it (apm-metrics §4).
var hostPorts = []struct{ host, port string }{
	{"net.peer.name", "net.peer.port"},
	{"server.address", "server.port"},
}

// externalAddress is the address of an external call (apm-metrics §4), or "".
func externalAddress(attrs pcommon.Map) string {
	if host, ok := firstString(attrs, "http.host"); ok && host != "" {
		return host
	}
	for _, hp := range hostPorts {
		host, ok := attrs.Get(hp.host)
		if !ok || host.AsString() == "" {
			continue
		}
		// Only the paired port counts, and only as a string: an integer port
		// leaves it empty, as the reference writes it.
		if port, ok := attrs.Get(hp.port); ok {
			return host.AsString() + ":" + port.Str()
		}
		return host.AsString()
	}
	for _, key := range []string{"http.url", "url.full"} {
		if raw, ok := firstString(attrs, key); ok {
			if u, err := url.Parse(raw); err == nil && u.Host != "" {
				return u.Host
			}
		}
	}
	return ""
}
