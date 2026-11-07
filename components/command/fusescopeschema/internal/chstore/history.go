// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/ClickHouse/ch-go"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrate"
)

var _ migrate.Store = (*Store)(nil)

// migrationDDL waits for every host to apply a migration's statement, and
// fails if one does not (§5.1).
var migrationDDL = []ch.Setting{
	{Key: "distributed_ddl_output_mode", Value: "throw"},
	{Key: "distributed_ddl_task_timeout", Value: "180"},
}

// ErrHistoryConflict: a history row is not the one this run wrote.
var ErrHistoryConflict = errors.New("the history row is not this run's")

func (s *Store) ExecDDL(ctx context.Context, statement string) error {
	return s.conn.ExecDiscard(ctx, statement, migrationDDL...)
}

const historyColumns = "toString(id), name, toString(checksum), toString(state), toString(adopted), toString(token)"

func parseRow(r []string) (migrate.Row, error) {
	id, err := strconv.ParseUint(r[0], 10, 32)
	if err != nil {
		return migrate.Row{}, err
	}
	token, err := strconv.ParseUint(r[5], 10, 64)
	if err != nil {
		return migrate.Row{}, err
	}
	return migrate.Row{ID: uint32(id), Name: r[1], Checksum: r[2], Done: r[3] == "done", Adopted: r[4] == "true", Token: token}, nil
}

// History reads fusescope_schema.migrations, ordered by id.
func (s *Store) History(ctx context.Context) ([]migrate.Row, error) {
	rows, err := s.conn.Rows(ctx, "SELECT "+historyColumns+" FROM "+s.table("migrations")+" ORDER BY id", 6)
	if err != nil {
		return nil, err
	}
	out := make([]migrate.Row, 0, len(rows))
	for _, r := range rows {
		row, err := parseRow(r)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (s *Store) row(ctx context.Context, id uint32) (migrate.Row, bool, error) {
	rows, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE id = %d", historyColumns, s.table("migrations"), id), 6)
	if err != nil || len(rows) == 0 {
		return migrate.Row{}, false, err
	}
	r, err := parseRow(rows[0])
	return r, err == nil, err
}

// confirm reads row id back and requires it to be want.
func (s *Store) confirm(ctx context.Context, want migrate.Row) error {
	got, ok, err := s.row(ctx, want.ID)
	if err != nil {
		return err
	}
	if !ok || got != want {
		return fmt.Errorf("%w: %04d reads back as %+v, want %+v", ErrHistoryConflict, want.ID, got, want)
	}
	return nil
}

func (s *Store) insertRow(ctx context.Context, r migrate.Row, state string, version, host string) error {
	finished := "NULL"
	if state == "done" {
		finished = "now64(3)"
	}
	return s.tryWrite(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, name, checksum, state, adopted, token, started_at, finished_at, tool_version, host) "+
			"SELECT %d, %s, %s, %s, %t, %d, now64(3), %s, %s, %s",
		s.table("migrations"), r.ID, quote(r.Name), quote(r.Checksum), quote(state), r.Adopted, r.Token, finished, quote(version), quote(host)))
}

// InsertStarted wraps migrate.ErrNotWritten only when a read after the insert
// shows no row of this run: no row with the id, or another run's.
func (s *Store) InsertStarted(ctx context.Context, r migrate.Row, version, host string) error {
	r.Done, r.Adopted = false, false
	insertErr := s.insertRow(ctx, r, "started", version, host)
	got, ok, err := s.row(ctx, r.ID)
	switch {
	case err != nil:
		return errors.Join(insertErr, fmt.Errorf("read the started row back: %w", err))
	case !ok:
		return errors.Join(migrate.ErrNotWritten, insertErr, fmt.Errorf("%04d is absent after the insert", r.ID))
	case got.Token != r.Token:
		return errors.Join(migrate.ErrNotWritten, ErrHistoryConflict, fmt.Errorf("%04d holds %+v", r.ID, got))
	case got != r:
		return fmt.Errorf("%w: %04d reads back as %+v, want %+v", ErrHistoryConflict, r.ID, got, r)
	}
	return insertErr
}

func (s *Store) InsertAdopted(ctx context.Context, r migrate.Row, version, host string) error {
	r.Done, r.Adopted = true, true
	if err := s.insertRow(ctx, r, "done", version, host); err != nil {
		return err
	}
	return s.confirm(ctx, r)
}

func (s *Store) MarkDone(ctx context.Context, id uint32, token uint64) error {
	before, ok, err := s.row(ctx, id)
	if err != nil {
		return err
	}
	if !ok || before.Done || before.Token != token {
		return fmt.Errorf("%w: %04d is %+v, want started by token %d", ErrHistoryConflict, id, before, token)
	}
	if err := s.tryWrite(ctx, fmt.Sprintf("ALTER TABLE %s UPDATE state = 'done', finished_at = now64(3) WHERE id = %d AND token = %d AND state = 'started'",
		s.table("migrations"), id, token)); err != nil {
		return err
	}
	before.Done = true
	return s.confirm(ctx, before)
}
