// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector"

import (
	"net/url"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// hostPorts are the host keys of an external call, in precedence order, each
// with the port key paired with it, if any (apm-metrics §4).
var hostPorts = []struct{ host, port string }{
	{"http.host", ""},
	{"net.peer.name", "net.peer.port"},
	{"server.address", "server.port"},
	{"net.peer.ip", "net.peer.port"},
	{"net.sock.peer.addr", "net.sock.peer.port"},
	{"network.peer.address", "network.peer.port"},
}

// appendExternalAddress appends the address of an external call to dst
// (apm-metrics §4). ok is false when the span has none and makes no series.
// Every value reads as Value.Str, so a non-string one is empty, as the
// reference writes it.
func appendExternalAddress(dst []byte, attrs pcommon.Map) (_ []byte, ok bool) {
	if _, ok := attrs.Get("rpc.system"); ok {
		service, hasService := attrs.Get("rpc.service")
		method, hasMethod := attrs.Get("rpc.method")
		if hasService {
			dst = append(dst, service.Str()...)
		}
		if hasMethod {
			dst = append(append(dst, '/'), method.Str()...)
		}
		if hasService || hasMethod {
			return dst, true
		}
	}
	for _, hp := range hostPorts {
		host, ok := attrs.Get(hp.host)
		if !ok {
			continue
		}
		dst = append(dst, host.Str()...)
		if hp.port != "" {
			if port, ok := attrs.Get(hp.port); ok {
				dst = append(append(dst, ':'), port.Str()...)
			}
		}
		return dst, true
	}
	for _, key := range []string{"http.url", "url.full"} {
		if v, ok := attrs.Get(key); ok {
			raw := v.Str()
			if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
				raw = "http://" + raw
			}
			u, err := url.Parse(raw)
			if err != nil {
				return dst, false
			}
			return append(dst, u.Host...), true
		}
	}
	if v, ok := attrs.Get("peer.service"); ok {
		return append(dst, v.Str()...), true
	}
	return dst, false
}
