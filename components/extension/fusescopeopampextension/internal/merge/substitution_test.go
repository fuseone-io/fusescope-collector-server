// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package merge

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// A pushed allow-listed block may not bring a confmap reference, which the
// collector would resolve, for example to a credential file, into a value it
// exports (ADR 0008 §2); references the base itself holds stay.
func TestPushedSubstitutionsRejected(t *testing.T) {
	const secret = "${file:/run/secrets/engine-token}"
	localRef := strings.Replace(base, "signozllmpricing: {}", "signozllmpricing: {output_attrs: {total: '${env:COST_ATTR}'}}", 1)
	tests := []struct {
		name, base, push string
		wantPaths        []string
	}{
		{"pricing output name", base,
			strings.Replace(base, "signozllmpricing: {}", "signozllmpricing: {output_attrs: {total: '"+secret+"'}}", 1),
			[]string{"processors.signozllmpricing.output_attrs.total"}},
		{"embedded in a value", base,
			strings.Replace(base, "signozspanmapper: {groups: []}", "signozspanmapper: {groups: [{id: 'x-"+secret+"'}]}", 1),
			[]string{"processors.signozspanmapper.groups[0].id"}},
		{"in a key", base,
			strings.Replace(base, "signozspanmapper: {groups: []}", "signozspanmapper: {'"+secret+"': 1}", 1),
			[]string{"processors.signozspanmapper"}},
		{"log pipeline block", base,
			strings.Replace(strings.Replace(base, "signozllmpricing: {}", "signozllmpricing: {}\n  signozlogspipeline/pipeline_a: {from: '${env:HOME}'}", 1),
				"[memory_limiter, batch]", "[memory_limiter, signozlogspipeline/pipeline_a, batch]", 1),
			[]string{"processors.signozlogspipeline/pipeline_a.from"}},
		{"escaped dollar is literal", base,
			strings.Replace(base, "signozllmpricing: {}", "signozllmpricing: {output_attrs: {total: '$${file:/x} $$$${y}'}}", 1), nil},
		{"base reference kept", localRef, localRef, nil},
		{"base reference kept beside a change", localRef,
			strings.Replace(localRef, "{total: '${env:COST_ATTR}'}", "{total: '${env:COST_ATTR}', in: c.in}", 1), nil},
		{"base reference moved", localRef,
			strings.Replace(localRef, "{total: '${env:COST_ATTR}'}", "{in: '${env:COST_ATTR}'}", 1),
			[]string{"processors.signozllmpricing.output_attrs.in"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Merge(parse(t, tt.base), parse(t, tt.push))
			var rej *Rejection
			switch {
			case tt.wantPaths == nil && err != nil:
				t.Fatalf("Merge rejected: %v", err)
			case tt.wantPaths != nil && !errors.As(err, &rej):
				t.Fatalf("Merge = %v, want a rejection of %v", err, tt.wantPaths)
			case tt.wantPaths != nil && !slices.Equal(rej.Paths, tt.wantPaths):
				t.Fatalf("rejected paths %v, want %v", rej.Paths, tt.wantPaths)
			case rej != nil && strings.Contains(rej.Error(), "secrets"):
				t.Fatal("the rejection echoes the value")
			}
		})
	}
}
