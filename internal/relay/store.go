package relay

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var ErrConflict = errors.New("idempotency key already used for a different request")
var ErrNotDead = errors.New("only a dead delivery can be replayed")
var ErrLeaseLost = errors.New("delivery lease is no longer owned")

type Job struct {
	ID        string `json:"id"`
	Key       string `json:"idempotency_key"`
	URL       string `json:"destination"`
	Payload   string `json:"payload"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	Cycle     int    `json:"cycle"`
	NextAt    int64  `json:"next_at"`
	CreatedAt int64  `json:"created_at"`
	LastError string `json:"last_error,omitempty"`
	Token     string `json:"-"`
	Hash      string `json:"-"`
}

type Attempt struct {
	Number     int    `json:"number"`
	Cycle      int    `json:"cycle"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at"`
	StatusCode int    `json:"status_code"`
	Error      string `json:"error,omitempty"`
}

type Store struct{ db *sql.DB }

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	db, err := sql.Open("sqlite3", u.String()+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL&_foreign_keys=on&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS deliveries (
 id TEXT PRIMARY KEY, idem_key TEXT NOT NULL UNIQUE, request_hash TEXT NOT NULL,
 destination TEXT NOT NULL, payload TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('pending','running','delivered','dead')),
 attempts INTEGER NOT NULL DEFAULT 0, cycle INTEGER NOT NULL DEFAULT 1,
 next_at INTEGER NOT NULL, created_at INTEGER NOT NULL,
 last_error TEXT NOT NULL DEFAULT '', lease_until INTEGER NOT NULL DEFAULT 0,
 lease_token TEXT NOT NULL DEFAULT '');
 CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(status,next_at);
 CREATE TABLE IF NOT EXISTS attempts (
 token TEXT PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES deliveries(id),
 number INTEGER NOT NULL, cycle INTEGER NOT NULL, started_at INTEGER NOT NULL,
 finished_at INTEGER NOT NULL DEFAULT 0, status_code INTEGER NOT NULL DEFAULT 0,
 error TEXT NOT NULL DEFAULT '');
 CREATE INDEX IF NOT EXISTS attempts_job ON attempts(delivery_id,cycle,number);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error                   { return s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

const cols = `id,idem_key,destination,payload,status,attempts,cycle,next_at,created_at,last_error,lease_token,request_hash`

type scanner interface{ Scan(...any) error }

func readJob(r scanner) (j Job, err error) {
	err = r.Scan(&j.ID, &j.Key, &j.URL, &j.Payload, &j.Status, &j.Attempts, &j.Cycle, &j.NextAt, &j.CreatedAt, &j.LastError, &j.Token, &j.Hash)
	return
}

func (s *Store) Enqueue(ctx context.Context, key, hash, dest, payload string, now time.Time) (Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `INSERT INTO deliveries(id,idem_key,request_hash,destination,payload,status,next_at,created_at) VALUES(?,?,?,?,?,'pending',?,?) ON CONFLICT(idem_key) DO NOTHING`, newID(), key, hash, dest, payload, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return Job{}, false, err
	}
	n, _ := r.RowsAffected()
	j, err := readJob(tx.QueryRowContext(ctx, "SELECT "+cols+" FROM deliveries WHERE idem_key=?", key))
	if err != nil {
		return j, false, err
	}
	if j.Hash != hash {
		return j, false, ErrConflict
	}
	err = tx.Commit()
	return j, n == 1, err
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	return readJob(s.db.QueryRowContext(ctx, "SELECT "+cols+" FROM deliveries WHERE id=?", id))
}
func (s *Store) List(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+cols+" FROM deliveries ORDER BY created_at DESC,id DESC LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, e := readJob(rows)
		if e != nil {
			return nil, e
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
func (s *Store) History(ctx context.Context, id string) ([]Attempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT number,cycle,started_at,finished_at,status_code,error FROM attempts WHERE delivery_id=? ORDER BY cycle,number`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	a := []Attempt{}
	for rows.Next() {
		var v Attempt
		if e := rows.Scan(&v.Number, &v.Cycle, &v.StartedAt, &v.FinishedAt, &v.StatusCode, &v.Error); e != nil {
			return nil, e
		}
		a = append(a, v)
	}
	return a, rows.Err()
}

// Claim is one IMMEDIATE transaction. An expired lease can be recovered by a
// different process. Every claim has a fresh token, preventing stale completion.
func (s *Store) Claim(ctx context.Context, now time.Time, lease time.Duration, max int) (Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()
	ms := now.UnixMilli()
	_, err = tx.ExecContext(ctx, `UPDATE attempts SET finished_at=?,error='worker lease expired; outcome unknown' WHERE token IN (SELECT lease_token FROM deliveries WHERE status='running' AND lease_until<=?) AND finished_at=0`, ms, ms)
	if err != nil {
		return Job{}, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE deliveries SET status=CASE WHEN attempts>=? THEN 'dead' ELSE 'pending' END,next_at=?,last_error='worker lease expired; outcome unknown',lease_token='',lease_until=0 WHERE status='running' AND lease_until<=?`, max, ms, ms)
	if err != nil {
		return Job{}, err
	}
	j, err := readJob(tx.QueryRowContext(ctx, "SELECT "+cols+" FROM deliveries WHERE status='pending' AND next_at<=? ORDER BY next_at,id LIMIT 1", ms))
	if errors.Is(err, sql.ErrNoRows) {
		if e := tx.Commit(); e != nil {
			return Job{}, e
		}
		return Job{}, sql.ErrNoRows
	}
	if err != nil {
		return j, err
	}
	j.Token = newID()
	j.Attempts++
	j.Status = "running"
	_, err = tx.ExecContext(ctx, `UPDATE deliveries SET status='running',attempts=?,lease_until=?,lease_token=? WHERE id=?`, j.Attempts, now.Add(lease).UnixMilli(), j.Token, j.ID)
	if err != nil {
		return j, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO attempts(token,delivery_id,number,cycle,started_at) VALUES(?,?,?,?,?)`, j.Token, j.ID, j.Attempts, j.Cycle, ms)
	if err != nil {
		return j, err
	}
	return j, tx.Commit()
}

func (s *Store) Complete(ctx context.Context, j Job, now time.Time, statusCode int, reason string, retryable bool, max int, delay time.Duration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	status := "delivered"
	if reason != "" {
		status = "dead"
		if retryable && j.Attempts < max {
			status = "pending"
		}
	}
	r, err := tx.ExecContext(ctx, `UPDATE deliveries SET status=?,next_at=?,last_error=?,lease_token='',lease_until=0 WHERE id=? AND status='running' AND lease_token=?`, status, now.Add(delay).UnixMilli(), reason, j.ID, j.Token)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return ErrLeaseLost
	}
	_, err = tx.ExecContext(ctx, `UPDATE attempts SET finished_at=?,status_code=?,error=? WHERE token=?`, now.UnixMilli(), statusCode, reason, j.Token)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) Replay(ctx context.Context, id string, now time.Time) error {
	r, err := s.db.ExecContext(ctx, `UPDATE deliveries SET status='pending',attempts=0,cycle=cycle+1,next_at=?,last_error='' WHERE id=? AND status='dead'`, now.UnixMilli(), id)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		if _, e := s.Get(ctx, id); e != nil {
			return e
		}
		return ErrNotDead
	}
	return nil
}
