package relay

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type API struct {
	Store  *Store
	Token  string
	Policy Policy
	Now    func() time.Time
}

var validKey = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,128}$`)
var validID = regexp.MustCompile(`^[a-f0-9]{32}$`)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
func failure(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (a API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if a.Store.Ping(r.Context()) != nil {
			failure(w, 503, "database unavailable")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/deliveries", a.enqueue)
	mux.HandleFunc("GET /v1/deliveries", a.list)
	mux.HandleFunc("GET /v1/deliveries/{id}", a.get)
	mux.HandleFunc("POST /v1/deliveries/{id}/replay", a.replay)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || len(a.Token) < 24 || subtle.ConstantTimeCompare([]byte(got), []byte(a.Token)) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				failure(w, 401, "unauthorized")
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}
func (a API) enqueue(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if !validKey.MatchString(key) {
		failure(w, 400, "Idempotency-Key must contain 1-128 letters, digits, _, ., :, or -")
		return
	}
	if ct := strings.Split(r.Header.Get("Content-Type"), ";")[0]; ct != "application/json" {
		failure(w, 415, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	var in struct {
		Destination string          `json:"destination"`
		Payload     json.RawMessage `json:"payload"`
	}
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(&in); err != nil {
		var big *http.MaxBytesError
		if errors.As(err, &big) {
			failure(w, 413, "request exceeds 64 KiB")
		} else {
			failure(w, 400, "invalid JSON request")
		}
		return
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		var big *http.MaxBytesError
		if errors.As(err, &big) {
			failure(w, 413, "request exceeds 64 KiB")
		} else {
			failure(w, 400, "request must contain one JSON object")
		}
		return
	}
	if len(in.Payload) == 0 {
		failure(w, 400, "payload is required")
		return
	}
	if err := a.Policy.Validate(in.Destination); err != nil {
		failure(w, 400, err.Error())
		return
	}
	var buf bytes.Buffer
	if json.Compact(&buf, in.Payload) != nil {
		failure(w, 400, "invalid payload")
		return
	}
	payload := buf.String()
	sum := sha256.Sum256([]byte(in.Destination + "\n" + payload))
	j, created, err := a.Store.Enqueue(r.Context(), key, hex.EncodeToString(sum[:]), in.Destination, payload, a.Now())
	if errors.Is(err, ErrConflict) {
		failure(w, 409, err.Error())
		return
	}
	if err != nil {
		failure(w, 500, "database error")
		return
	}
	code := 200
	if created {
		code = 202
	}
	writeJSON(w, code, map[string]any{"delivery": j, "created": created})
}
func (a API) list(w http.ResponseWriter, r *http.Request) {
	j, e := a.Store.List(r.Context())
	if e != nil {
		failure(w, 500, "database error")
		return
	}
	writeJSON(w, 200, map[string]any{"deliveries": j, "limit": 100})
}
func (a API) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID.MatchString(id) {
		failure(w, 404, "not found")
		return
	}
	j, e := a.Store.Get(r.Context(), id)
	if errors.Is(e, sql.ErrNoRows) {
		failure(w, 404, "not found")
		return
	}
	if e != nil {
		failure(w, 500, "database error")
		return
	}
	attempts, e := a.Store.History(r.Context(), id)
	if e != nil {
		failure(w, 500, "database error")
		return
	}
	writeJSON(w, 200, map[string]any{"delivery": j, "attempts": attempts})
}
func (a API) replay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID.MatchString(id) {
		failure(w, 404, "not found")
		return
	}
	e := a.Store.Replay(r.Context(), id, a.Now())
	if errors.Is(e, sql.ErrNoRows) {
		failure(w, 404, "not found")
		return
	}
	if errors.Is(e, ErrNotDead) {
		failure(w, 409, e.Error())
		return
	}
	if e != nil {
		failure(w, 500, "database error")
		return
	}
	writeJSON(w, 202, map[string]string{"status": "pending", "id": id})
}
