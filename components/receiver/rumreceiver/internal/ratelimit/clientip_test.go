// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// The first three TestClientIP cases are moved from fuseone-io/fusescope
// pkg/ratelimit/ratelimit_test.go at 6c8991257 (MIT, see LICENSE-MIT in the
// module root); the trusted-proxy and grouping cases are new (§7, G2, G6).

package ratelimit

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func prefixes(cidrs ...string) TrustedProxies {
	out := TrustedProxies{}
	for _, c := range cidrs {
		out = append(out, netip.MustParsePrefix(c))
	}
	return out
}

func TestClientIP(t *testing.T) {
	proxies := prefixes("10.0.0.0/8", "2001:db8:aaaa::/48")
	testCases := []struct {
		name       string
		proxies    TrustedProxies
		remoteAddr string
		forwarded  []string
		expected   string
	}{
		{name: "RemoteAddr_PortStripped", remoteAddr: "10.0.0.1:5123", expected: "10.0.0.1"},
		{name: "RemoteAddrIPv6_PortStripped", remoteAddr: "[2001:db8::1]:443", expected: "2001:db8::1"},
		{name: "RemoteAddrWithoutPort_Kept", remoteAddr: "10.0.0.2", expected: "10.0.0.2"},
		{name: "RemoteAddrMapped_Unmapped", remoteAddr: "[::ffff:203.0.113.9]:80", expected: "203.0.113.9"},
		{name: "RemoteAddrUnparseable_Invalid", remoteAddr: "pipe", expected: "invalid IP"},
		{name: "NoProxiesConfigured_ForwardedIgnored", remoteAddr: "10.0.0.3:80", forwarded: []string{"203.0.113.7"}, expected: "10.0.0.3"},
		{name: "UntrustedPeer_ForwardedIgnored", proxies: proxies, remoteAddr: "198.51.100.1:80", forwarded: []string{"203.0.113.7"}, expected: "198.51.100.1"},
		{name: "TrustedPeer_RightmostUntrusted", proxies: proxies, remoteAddr: "10.0.0.4:80", forwarded: []string{" 203.0.113.8 , 198.51.100.2"}, expected: "198.51.100.2"},
		{name: "TrustedChain_Skipped", proxies: proxies, remoteAddr: "10.0.0.4:80", forwarded: []string{"203.0.113.8, 10.1.1.1 ,10.2.2.2"}, expected: "203.0.113.8"},
		{name: "SeveralHeaderLines_WalkedAsOne", proxies: proxies, remoteAddr: "10.0.0.4:80", forwarded: []string{"203.0.113.8", "10.9.9.9"}, expected: "203.0.113.8"},
		{name: "SpoofedLeftmostBehindClient_NotTaken", proxies: proxies, remoteAddr: "10.0.0.4:80", forwarded: []string{"1.2.3.4, 203.0.113.8"}, expected: "203.0.113.8"},
		{name: "TrustedButMissing_Peer", proxies: proxies, remoteAddr: "10.0.0.5:80", expected: "10.0.0.5"},
		{name: "Garbage_LastTrustedSeen", proxies: proxies, remoteAddr: "10.0.0.6:80", forwarded: []string{"203.0.113.8, unknown, 10.3.3.3"}, expected: "10.3.3.3"},
		{name: "GarbageRightmost_Peer", proxies: proxies, remoteAddr: "10.0.0.6:80", forwarded: []string{"203.0.113.8, 10.3.3.3:4711"}, expected: "10.0.0.6"},
		{name: "EmptyEntry_EndsWalk", proxies: proxies, remoteAddr: "10.0.0.6:80", forwarded: []string{"203.0.113.8,"}, expected: "10.0.0.6"},
		{name: "AllTrusted_LastTrustedSeen", proxies: proxies, remoteAddr: "10.0.0.7:80", forwarded: []string{"10.4.4.4, 10.5.5.5"}, expected: "10.4.4.4"},
		{name: "IPv6Proxy_Trusted", proxies: proxies, remoteAddr: "[2001:db8:aaaa::1]:443", forwarded: []string{"2001:db8:1:2::3"}, expected: "2001:db8:1:2::3"},
		{name: "MappedForwarded_Unmapped", proxies: proxies, remoteAddr: "10.0.0.8:80", forwarded: []string{"::ffff:203.0.113.10"}, expected: "203.0.113.10"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.proxies.ClientIP(tc.remoteAddr, tc.forwarded).String())
		})
	}
}

func TestGroup(t *testing.T) {
	testCases := []struct {
		name     string
		addr     string
		expected string
	}{
		{name: "IPv4_Own", addr: "203.0.113.7", expected: "203.0.113.7"},
		{name: "IPv4Mapped_Own", addr: "::ffff:203.0.113.7", expected: "203.0.113.7"},
		{name: "IPv6_Slash64", addr: "2001:db8:1:2:aaaa:bbbb:cccc:dddd", expected: "2001:db8:1:2::"},
		{name: "IPv6Zone_Dropped", addr: "fe80::1%eth0", expected: "fe80::"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, Group(netip.MustParseAddr(tc.addr)).String())
		})
	}
	assert.Equal(t, Group(netip.MustParseAddr("2001:db8:1:2::1")), Group(netip.MustParseAddr("2001:db8:1:2:ffff::9")), "one /64 is one group")
	assert.NotEqual(t, Group(netip.MustParseAddr("2001:db8:1:2::1")), Group(netip.MustParseAddr("2001:db8:1:3::1")))
	assert.False(t, Group(netip.Addr{}).IsValid())
}

func BenchmarkClientIP(b *testing.B) {
	proxies := prefixes("10.0.0.0/8")
	forwarded := []string{"203.0.113.8, 10.1.1.1"}
	b.ReportAllocs()
	for b.Loop() {
		Group(proxies.ClientIP("10.0.0.4:80", forwarded))
	}
}
