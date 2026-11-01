// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	mathrand "math/rand/v2"
	"time"

	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.uber.org/zap"
)

// requestID identifies one export request across its retry attempts, which
// all run within one push call. Equal content sent twice is two requests.
type requestID struct {
	nonce   [16]byte
	started time.Time
}

func newRequestID(now time.Time) requestID {
	id := requestID{started: now}
	_, _ = rand.Read(id.nonce[:])
	return id
}

// token is the base of the request's insert_deduplication_token.
func (id requestID) token() string { return hex.EncodeToString(id.nonce[:]) }

// random returns the request's ID generator. Every attempt replays the same
// stream, so a retry rebuilds the same random IDs.
func (id requestID) random() func([]byte) {
	rng := mathrand.NewChaCha8(sha256.Sum256(id.nonce[:]))
	return func(b []byte) { _, _ = rng.Read(b) }
}

// retry runs attempt until it succeeds, fails permanently or cfg gives up, with
// the exponential backoff of retry_on_failure; stop ends the wait between
// attempts, as on shutdown. The exporter retries itself, instead of the
// exporter helper, so that every attempt of a request shares one requestID
// without the exporter keeping state between push calls.
func retry(ctx context.Context, cfg configretry.BackOffConfig, logger *zap.Logger, stop <-chan struct{}, attempt func(context.Context) error) error {
	// The first attempt counts toward max_elapsed_time, as in the exporter helper.
	var deadline time.Time
	if cfg.MaxElapsedTime > 0 {
		deadline = time.Now().Add(cfg.MaxElapsedTime)
	}
	err := attempt(ctx)
	if err == nil || !cfg.Enabled || consumererror.IsPermanent(err) {
		return err
	}
	interval := cfg.InitialInterval
	for {
		delay := jitter(interval, cfg.RandomizationFactor)
		interval = min(time.Duration(float64(interval)*cfg.Multiplier), cfg.MaxInterval)
		next := time.Now().Add(delay)
		if !deadline.IsZero() && next.After(deadline) {
			return fmt.Errorf("no more retries left: %w", err)
		}
		if d, ok := ctx.Deadline(); ok && next.After(d) {
			return fmt.Errorf("request will be canceled before the next retry: %w", err)
		}
		logger.Info("Exporting failed. Will retry the request after interval.", zap.Error(err), zap.Duration("interval", delay))
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("request canceled while waiting to retry: %w", err)
		case <-stop:
			t.Stop()
			return fmt.Errorf("exporter shut down while waiting to retry: %w", err)
		case <-t.C:
		}
		if err = attempt(ctx); err == nil || consumererror.IsPermanent(err) {
			return err
		}
	}
}

// jitter spreads d by ±factor, as configretry's randomization_factor.
func jitter(d time.Duration, factor float64) time.Duration {
	if factor <= 0 {
		return d
	}
	delta := factor * float64(d)
	return time.Duration(float64(d) - delta + mathrand.Float64()*2*delta)
}
