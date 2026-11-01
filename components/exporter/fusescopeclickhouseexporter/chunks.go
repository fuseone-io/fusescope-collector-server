// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

// rowOverhead covers the fixed columns of one span or log row.
const rowOverhead = 256

// chunker cuts the rows of a traces or logs batch into chunks: every span and
// log record copies its resource, so a large resource over many small records
// expands far beyond the request (traces §1, logs §1). It counts those copies;
// the record's own data is bounded by the request.
type chunker struct {
	maxChunkBytes, expanded int
	// emit inserts the current chunk; err is its first failure.
	emit func() error
	err  error
}

func newChunker() chunker { return chunker{maxChunkBytes: defaultChunkBytes} }

func (c *chunker) begin(emit func() error) {
	c.emit, c.err, c.expanded = emit, nil, 0
}

// add counts one row of copied bytes, and inserts the chunk once it exceeds
// maxChunkBytes, calling reset to empty it. It reports whether encoding may go on.
func (c *chunker) add(copied int, reset func()) bool {
	c.expanded += copied + rowOverhead
	if c.expanded <= c.maxChunkBytes || c.emit == nil {
		return true
	}
	if c.err = c.emit(); c.err != nil {
		return false
	}
	reset()
	c.expanded = 0
	return true
}
