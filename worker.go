package relay

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Worker struct {
	Store                      *Store
	Client                     *http.Client
	Policy                     Policy
	Secret                     string
	Now                        func() time.Time
	MaxAttempts                int
	BaseDelay, MaxDelay, Lease time.Duration
}

func Backoff(attempt int, base, cap time.Duration) time.Duration {
	d := base
	for n := 1; n < attempt; n++ {
		if d >= cap/2 {
			return cap
		}
		d *= 2
	}
	if d > cap {
		return cap
	}
	return d
}

// Step performs at most one delivery, making worker behavior deterministically
// testable with an injected clock. The CLI calls it continuously.
func (w *Worker) Step(ctx context.Context) (bool, error) {
	now := w.Now()
	j, e := w.Store.Claim(ctx, now, w.Lease, w.MaxAttempts)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	code := 0
	reason := ""
	retry := false
	delay := Backoff(j.Attempts, w.BaseDelay, w.MaxDelay)
	if e = w.Policy.Validate(j.URL); e != nil {
		reason = "destination rejected by policy"
	} else {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.URL, strings.NewReader(j.Payload))
		if err != nil {
			return true, err
		}
		ts := now.UTC().Format(time.RFC3339)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "RelayKit/0.1")
		req.Header.Set("X-Relay-ID", j.ID)
		req.Header.Set("X-Relay-Timestamp", ts)
		req.Header.Set("X-Relay-Attempt", strconv.Itoa(j.Attempts))
		req.Header.Set("X-Relay-Signature", Signature(w.Secret, ts, j.ID, []byte(j.Payload)))
		res, err := w.Client.Do(req)
		if err != nil {
			reason = "network or timeout error"
			retry = true
		} else {
			code = res.StatusCode
			io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()
			if code < 200 || code >= 300 {
				reason = fmt.Sprintf("HTTP %d", code)
				retry = code == 408 || code == 425 || code == 429 || code >= 500
				if retry {
					if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds > 0 && seconds <= int(w.MaxDelay/time.Second) {
						if ra := time.Duration(seconds) * time.Second; ra > delay {
							delay = ra
						}
					}
				}
			}
		}
	}
	return true, w.Store.Complete(ctx, j, w.Now(), code, reason, retry, w.MaxAttempts, delay)
}
