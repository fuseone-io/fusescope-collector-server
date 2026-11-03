// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ratelimit // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/ratelimit"

import (
	"net/netip"
	"strings"
)

// TrustedProxies are the peers whose X-Forwarded-For is read (§7, G2).
type TrustedProxies []netip.Prefix

func (t TrustedProxies) trusts(a netip.Addr) bool {
	for _, p := range t {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// ClientIP is the peer address, unless the peer is a trusted proxy: then the
// X-Forwarded-For entries (every header line, comma-separated) are walked
// right to left, skipping trusted addresses, and the first other address is
// the client. An entry that is not an IP address ends the walk with the last
// trusted address seen. The leftmost entry, which the client writes, is
// never taken on its own.
func (t TrustedProxies) ClientIP(remoteAddr string, forwardedFor []string) netip.Addr {
	peer := parsePeer(remoteAddr)
	if len(t) == 0 || !peer.IsValid() || !t.trusts(peer) {
		return peer
	}
	last := peer
	for i := len(forwardedFor) - 1; i >= 0; i-- {
		line := forwardedFor[i]
		for {
			cut := strings.LastIndexByte(line, ',')
			entry := strings.TrimSpace(line[cut+1:])
			addr, err := netip.ParseAddr(entry)
			if err != nil {
				return last
			}
			addr = addr.Unmap()
			if !t.trusts(addr) {
				return addr
			}
			last = addr
			if cut < 0 {
				break
			}
			line = line[:cut]
		}
	}
	return last
}

func parsePeer(remoteAddr string) netip.Addr {
	if ap, err := netip.ParseAddrPort(remoteAddr); err == nil {
		return ap.Addr().Unmap()
	}
	if a, err := netip.ParseAddr(remoteAddr); err == nil {
		return a.Unmap()
	}
	return netip.Addr{}
}

// Group is the rate-limit group of a client: an IPv4 address on its own, an
// IPv6 address by its /64 (§7, G6).
func Group(a netip.Addr) netip.Addr {
	a = a.Unmap()
	if !a.Is6() {
		return a
	}
	p, err := a.WithZone("").Prefix(64)
	if err != nil {
		return netip.Addr{}
	}
	return p.Addr()
}
