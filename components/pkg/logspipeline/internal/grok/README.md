# internal/grok

Imported from [`github.com/vjeantet/grok`](https://github.com/vjeantet/grok) v1.0.1
(Apache-2.0, [LICENSE](LICENSE)), copied from the module as `go mod download` fetches it
(`h1:2rhIR7J4gThTgcZ1m2JY4TrJZNgjn985U28kT2wQrJ4=`). Used by the `grok_parser` operator of
`signozlogspipeline` ([spec](../../../../../docs/spec/signozlogspipeline.md) §3.2).

Changes from upstream:

- every `.go` file starts with a copyright, SPDX and provenance header;
- `grok_test.go`, `TestConcurentParse`: `t.Fatalf` inside goroutines became `t.Errorf`,
  which `go vet` requires;
- `patternfiles.go` and its test (FuseOne) embed `patterns/` with `go:embed`. The files are
  not loaded by default; the default pattern set is `patterns.go`, as upstream.

`grok.go`, `graph.go`, `patterns.go` and `patterns/` are otherwise unchanged.

Go's `regexp` (RE2) matches in time linear in the input, without backtracking, so a pattern
cannot take exponential time; there is no per-match timeout to set.
