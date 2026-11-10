package relay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "test-only-api-token-32-characters"
const testSecret = "test-only-signing-secret-32-chars"

func testStore(t *testing.T) *Store {
	t.Helper()
	s, e := Open(filepath.Join(t.TempDir(), "queue.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func insert(t *testing.T, s *Store, dest string, now time.Time) Job {
	t.Helper()
	j, _, e := s.Enqueue(context.Background(), newID(), "hash", dest, `{"event":"order.created"}`, now)
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func worker(s *Store, now *time.Time) *Worker {
	p := Policy{AllowLocalDemo: true}
	return &Worker{Store: s, Client: NewHTTPClient(p, time.Second), Policy: p, Secret: testSecret, Now: func() time.Time { return *now }, MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Minute, Lease: 5 * time.Second}
}

func TestRetryDeliveryAndSignature(t *testing.T) {
	s := testStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if !VerifySignature(testSecret, r.Header.Get("X-Relay-Timestamp"), r.Header.Get("X-Relay-ID"), b, r.Header.Get("X-Relay-Signature"), now, time.Minute) {
			t.Error("invalid signature")
		}
		if calls.Add(1) <= 2 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer receiver.Close()
	j := insert(t, s, receiver.URL, now)
	w := worker(s, &now)
	for i, d := range []time.Duration{0, time.Second, 2 * time.Second} {
		now = now.Add(d)
		did, e := w.Step(context.Background())
		if e != nil || !did {
			t.Fatalf("step %d %v %v", i, did, e)
		}
		if i < 2 {
			did, e = w.Step(context.Background())
			if e != nil || did {
				t.Fatal("retried before backoff elapsed")
			}
		}
	}
	got, _ := s.Get(context.Background(), j.ID)
	if got.Status != "delivered" || got.Attempts != 3 {
		t.Fatalf("%+v", got)
	}
	h, _ := s.History(context.Background(), j.ID)
	if len(h) != 3 || h[0].StatusCode != 503 || h[2].StatusCode != 204 {
		t.Fatalf("%+v", h)
	}
}
func TestDeadLetterAndReplay(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	var status atomic.Int32
	status.Store(503)
	r := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(int(status.Load())) }))
	defer r.Close()
	j := insert(t, s, r.URL, now)
	w := worker(s, &now)
	for i := 0; i < 3; i++ {
		now = now.Add(time.Minute)
		if _, e := w.Step(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	j, _ = s.Get(context.Background(), j.ID)
	if j.Status != "dead" {
		t.Fatalf("%+v", j)
	}
	if e := s.Replay(context.Background(), j.ID, now); e != nil {
		t.Fatal(e)
	}
	status.Store(200)
	if _, e := w.Step(context.Background()); e != nil {
		t.Fatal(e)
	}
	j, _ = s.Get(context.Background(), j.ID)
	if j.Status != "delivered" || j.Cycle != 2 || j.Attempts != 1 {
		t.Fatalf("%+v", j)
	}
	h, _ := s.History(context.Background(), j.ID)
	if len(h) != 4 || h[3].Cycle != 2 {
		t.Fatal(h)
	}
	if e := s.Replay(context.Background(), j.ID, now); !errors.Is(e, ErrNotDead) {
		t.Fatalf("replayed delivered job: %v", e)
	}
}
func TestPermanentErrorDoesNotRetry(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	r := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400) }))
	defer r.Close()
	j := insert(t, s, r.URL, now)
	if _, e := worker(s, &now).Step(context.Background()); e != nil {
		t.Fatal(e)
	}
	j, _ = s.Get(context.Background(), j.ID)
	if j.Status != "dead" || j.Attempts != 1 {
		t.Fatal(j)
	}
}
func TestDurabilityAndLeaseFencing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite")
	s, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	j := insert(t, s, "https://example.com/hook", now)
	first, e := s.Claim(context.Background(), now, time.Second, 3)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	next := now.Add(2 * time.Second)
	second, e := s.Claim(context.Background(), next, time.Second, 3)
	if e != nil {
		t.Fatal(e)
	}
	if second.ID != j.ID || first.Token == second.Token || second.Attempts != 2 {
		t.Fatal(second)
	}
	if e = s.Complete(context.Background(), first, next, 200, "", false, 3, 0); !errors.Is(e, ErrLeaseLost) {
		t.Fatalf("stale worker committed: %v", e)
	}
	if e = s.Complete(context.Background(), second, next, 200, "", false, 3, 0); e != nil {
		t.Fatal(e)
	}
	h, _ := s.History(context.Background(), j.ID)
	if len(h) != 2 || !strings.Contains(h[0].Error, "lease expired") {
		t.Fatal(h)
	}
}
func TestExpiredFinalAttemptBecomesDead(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	j := insert(t, s, "https://example.com", now)
	if _, e := s.Claim(context.Background(), now, time.Second, 1); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Claim(context.Background(), now.Add(2*time.Second), time.Second, 1); !errors.Is(e, sql.ErrNoRows) {
		t.Fatal(e)
	}
	j, _ = s.Get(context.Background(), j.ID)
	if j.Status != "dead" {
		t.Fatal(j)
	}
}

func TestConcurrentIdempotencyAndClaims(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite")
	a, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	stores := []*Store{a, b}
	var created atomic.Int32
	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, isNew, e := stores[i%2].Enqueue(context.Background(), "same-key", "same-hash", "https://example.com", "{}", now)
			if e != nil {
				t.Error(e)
			}
			if isNew {
				created.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if created.Load() != 1 {
		t.Fatal(created.Load())
	}
	var claims atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := stores[i%2].Claim(context.Background(), now, time.Second, 3)
			if e == nil {
				claims.Add(1)
			} else if !errors.Is(e, sql.ErrNoRows) {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	if claims.Load() != 1 {
		t.Fatal(claims.Load())
	}
	if _, _, e := a.Enqueue(context.Background(), "same-key", "different-hash", "https://example.com", "{}", now); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
}

func TestAPIAuthenticationIdempotencyAndValidation(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	server := httptest.NewServer(API{Store: s, Token: testToken, Now: func() time.Time { return now }}.Handler())
	defer server.Close()
	call := func(method, path, body, key, token string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		var data map[string]any
		json.NewDecoder(resp.Body).Decode(&data)
		return resp.StatusCode, data
	}
	if c, _ := call("GET", "/v1/deliveries", "", "", "wrong"); c != 401 {
		t.Fatal(c)
	}
	body := `{"destination":"https://example.com/hook","payload":{"ok":true}}`
	c, data := call("POST", "/v1/deliveries", body, "order:123", testToken)
	if c != 202 {
		t.Fatal(c, data)
	}
	id := data["delivery"].(map[string]any)["id"].(string)
	if c, _ = call("POST", "/v1/deliveries", body, "order:123", testToken); c != 200 {
		t.Fatal(c)
	}
	if c, _ = call("POST", "/v1/deliveries", strings.Replace(body, "true", "false", 1), "order:123", testToken); c != 409 {
		t.Fatal(c)
	}
	if c, _ = call("GET", "/v1/deliveries/"+id, "", "", testToken); c != 200 {
		t.Fatal(c)
	}
	if c, _ = call("POST", "/v1/deliveries/"+id+"/replay", "", "", testToken); c != 409 {
		t.Fatal(c)
	}
	for _, bad := range []string{`{"destination":"http://127.0.0.1","payload":{}}`, body + "{}", `{"destination":"https://example.com"}`, `{"destination":"https://example.com","payload":{},"surprise":1}`} {
		if c, _ = call("POST", "/v1/deliveries", bad, newID(), testToken); c != 400 {
			t.Fatalf("%d %s", c, bad)
		}
	}
	if c, _ = call("POST", "/v1/deliveries", fmt.Sprintf(`{"destination":"https://example.com","payload":"%s"}`, strings.Repeat("x", 70000)), "large", testToken); c != 413 {
		t.Fatal(c)
	}
	if c, _ = call("POST", "/v1/deliveries", body+strings.Repeat(" ", 70000), "large-trailing", testToken); c != 413 {
		t.Fatal(c)
	}
}

func TestSSRFPolicyAndDialer(t *testing.T) {
	p := Policy{}
	for _, s := range []string{"http://example.com", "https://127.0.0.1", "https://10.0.0.1", "https://169.254.169.254", "https://[::1]", "https://[::ffff:127.0.0.1]", "https://[2001:db8::1]", "https://localhost", "https://abc.local", "https://user:pass@example.com", "https://example.com:8443", "https://example.com/#x"} {
		if p.Validate(s) == nil {
			t.Error("allowed", s)
		}
	}
	for _, s := range []string{"https://example.com/hook", "https://8.8.8.8", "https://[2001:4860:4860::8888]"} {
		if p.Validate(s) != nil {
			t.Error("blocked", s)
		}
	}
	for _, ip := range []string{"100.64.0.1", "192.0.2.1", "192.88.99.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "fc00::1", "fe80::1", "fec0::1", "100::1", "5f00::1", "3fff::1", "2001:20::1", "64:ff9b::a00:1"} {
		if publicIP(netip.MustParseAddr(ip)) {
			t.Error("public classification", ip)
		}
	}
	local := Policy{AllowLocalDemo: true}
	if local.Validate("http://127.0.0.1:9090/hook") != nil {
		t.Fatal("demo denied")
	}
	if local.Validate("http://192.168.0.1") == nil {
		t.Fatal("demo must not enable LAN")
	}
	client := NewHTTPClient(p, time.Second)
	if _, e := client.Get("https://127.0.0.1:4444"); e == nil {
		t.Fatal("dialer connected to loopback")
	}
}
func TestRedirectsAreNotFollowed(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetHits.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer source.Close()
	res, e := NewHTTPClient(Policy{AllowLocalDemo: true}, time.Second).Get(source.URL)
	if e != nil {
		t.Fatal(e)
	}
	res.Body.Close()
	if res.StatusCode != 302 || targetHits.Load() != 0 {
		t.Fatal("followed redirect")
	}
}
func TestSignatureRejectsTamperingAndReplay(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ts := now.Format(time.RFC3339)
	sig := Signature(testSecret, ts, "id", []byte("{}"))
	if !VerifySignature(testSecret, ts, "id", []byte("{}"), sig, now, time.Minute) {
		t.Fatal("good rejected")
	}
	if VerifySignature(testSecret, ts, "id", []byte("[]"), sig, now, time.Minute) || VerifySignature(testSecret, ts, "id", []byte("{}"), sig, now.Add(time.Hour), time.Minute) {
		t.Fatal("bad accepted")
	}
}
func TestBackoffIsCapped(t *testing.T) {
	for _, tc := range []struct {
		n    int
		want time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {100, time.Minute}} {
		if got := Backoff(tc.n, time.Second, time.Minute); got != tc.want {
			t.Errorf("%d: %v", tc.n, got)
		}
	}
}
