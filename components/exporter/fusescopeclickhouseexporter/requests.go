// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	mathrand "math/rand/v2"
	"sync"
	"time"
)

// requestID identifies one export request across its retries. The exporter
// helper retries a request by passing the same pdata value again, so the value
// itself is the key; equal content sent twice is two requests.
type requestID struct {
	nonce   [16]byte
	started time.Time
	expires time.Time
}

// token is the base of the request's insert_deduplication_token.
func (id requestID) token() string { return hex.EncodeToString(id.nonce[:]) }

// random returns the request's ID generator. Every attempt replays the same
// stream, so a retry rebuilds the same random IDs.
func (id requestID) random() func([]byte) {
	rng := mathrand.NewChaCha8(sha256.Sum256(id.nonce[:]))
	return func(b []byte) { _, _ = rng.Read(b) }
}

// maxRequestIDs bounds the identities kept for requests that may be retried.
const maxRequestIDs = 100_000

// requestIDs holds the identities of requests that may still be retried.
// Identities older than ttl are dropped, as the exporter helper has given up
// on them; ttl 0 keeps them until done. Above max identities the oldest go, at
// worst turning a late retry into a new request.
type requestIDs[D comparable] struct {
	mu  sync.Mutex
	ids map[D]requestID
	ttl time.Duration
	max int
	now func() time.Time
	// quit stops the expiry loop of start.
	quit chan struct{}
	wg   sync.WaitGroup
}

func newRequestIDs[D comparable](ttl time.Duration) *requestIDs[D] {
	return &requestIDs[D]{ids: map[D]requestID{}, ttl: ttl, max: maxRequestIDs, now: time.Now}
}

// start expires identities every interval, also while no request arrives:
// each identity keeps its batch referenced.
func (r *requestIDs[D]) start(interval time.Duration) {
	if r.ttl <= 0 || interval <= 0 || r.quit != nil {
		return
	}
	r.quit = make(chan struct{})
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-r.quit:
				return
			case <-t.C:
				r.expire()
			}
		}
	}()
}

func (r *requestIDs[D]) stop() {
	if r.quit != nil {
		close(r.quit)
		r.wg.Wait()
		r.quit = nil
	}
}

func (r *requestIDs[D]) expire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for k, id := range r.ids {
		if now.After(id.expires) {
			delete(r.ids, k)
		}
	}
}

func (r *requestIDs[D]) get(d D) requestID {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if id, ok := r.ids[d]; ok {
		return id
	}
	if len(r.ids) >= r.max {
		var oldest D
		first := true
		for k, id := range r.ids {
			if first || id.started.Before(r.ids[oldest].started) {
				oldest, first = k, false
			}
		}
		delete(r.ids, oldest)
	}
	id := requestID{started: now, expires: now.Add(r.ttl)}
	_, _ = rand.Read(id.nonce[:])
	r.ids[d] = id
	return id
}

// done forgets a request that succeeded or will not be retried.
func (r *requestIDs[D]) done(d D) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.ids, d)
}
