// receiver is a deliberately flaky LOCAL demo endpoint. Do not expose it publicly.
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Yermukhambet-raiymbek/relaykit/internal/relay"
)

func main() {
	secret := os.Getenv("RELAY_SIGNING_SECRET")
	if len(secret) < 24 {
		log.Fatal("set RELAY_SIGNING_SECRET to at least 24 characters")
	}
	var mu sync.Mutex
	counts := map[string]int{}
	processed := map[string]bool{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		body, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
		if e != nil {
			http.Error(w, "body too large", 413)
			return
		}
		id := r.Header.Get("X-Relay-ID")
		if id == "" || !relay.VerifySignature(secret, r.Header.Get("X-Relay-Timestamp"), id, body, r.Header.Get("X-Relay-Signature"), time.Now(), 5*time.Minute) {
			http.Error(w, "invalid signature", 401)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		counts[id]++
		if counts[id] <= 2 {
			log.Printf("%s simulated transient failure %d", id, counts[id])
			http.Error(w, "simulated outage", 503)
			return
		}
		duplicate := processed[id]
		processed[id] = true
		log.Printf("%s accepted, duplicate=%v", id, duplicate)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"accepted": true, "duplicate": duplicate})
	})
	log.Print("Flaky demo receiver on 127.0.0.1:9090; first two attempts return 503")
	s := &http.Server{Addr: "127.0.0.1:9090", Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	log.Fatal(s.ListenAndServe())
}
