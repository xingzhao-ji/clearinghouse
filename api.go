package clearinghouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type API struct {
	store    *Store
	tokens   map[[32]byte]string
	requests atomic.Uint64
	failures atomic.Uint64
	nanos    atomic.Uint64
}

func NewAPI(store *Store, tokens map[string]string) http.Handler {
	a := &API{store: store, tokens: map[[32]byte]string{}}
	for token, tenant := range tokens {
		if token != "" {
			a.tokens[sha256.Sum256([]byte(token))] = tenant
		}
	}
	return a
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func errorResponse(w http.ResponseWriter, e error) int {
	status := http.StatusServiceUnavailable
	message := "dependency unavailable; retry with the same idempotency key"
	switch {
	case errors.Is(e, ErrInvalid):
		status = 400
		message = e.Error()
	case errors.Is(e, ErrNotFound):
		status = 404
		message = e.Error()
	case errors.Is(e, ErrConflict), errors.Is(e, ErrFunds), errors.Is(e, ErrCurrency), errors.Is(e, ErrCapacity):
		status = 409
		message = e.Error()
	}
	respond(w, status, map[string]string{"error": message})
	return status
}

// decodeTransfer rejects ambiguous duplicate names as well as unknown/missing/null fields.
func decodeTransfer(body io.Reader) (TransferRequest, error) {
	var r TransferRequest
	dec := json.NewDecoder(body)
	token, e := dec.Token()
	if e != nil || token != json.Delim('{') {
		return r, ErrInvalid
	}
	seen := map[string]bool{}
	for dec.More() {
		token, e = dec.Token()
		if e != nil {
			return r, ErrInvalid
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return r, ErrInvalid
		}
		seen[key] = true
		var raw json.RawMessage
		if e = dec.Decode(&raw); e != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return r, ErrInvalid
		}
		switch key {
		case "from":
			e = json.Unmarshal(raw, &r.From)
		case "to":
			e = json.Unmarshal(raw, &r.To)
		case "amount":
			e = json.Unmarshal(raw, &r.Amount)
		default:
			return r, ErrInvalid
		}
		if e != nil {
			return r, ErrInvalid
		}
	}
	if token, e = dec.Token(); e != nil || token != json.Delim('}') || len(seen) != 3 {
		return r, ErrInvalid
	}
	if e = dec.Decode(new(any)); e != io.EOF {
		return r, ErrInvalid
	}
	return r, nil
}
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	status := 200
	var id [12]byte
	rand.Read(id[:])
	requestID := hex.EncodeToString(id[:])
	w.Header().Set("X-Request-ID", requestID)
	defer func() {
		a.requests.Add(1)
		a.nanos.Add(uint64(time.Since(started)))
		if status >= 500 {
			a.failures.Add(1)
		}
		slog.Info("http", "method", r.Method, "status", status, "duration_ms", time.Since(started).Milliseconds(), "request_id", requestID)
	}()
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	if r.URL.Path == "/healthz" && r.Method == "GET" {
		if e := a.store.Pool.Ping(ctx); e != nil {
			status = errorResponse(w, e)
			return
		}
		respond(w, 200, map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path == "/metrics" && r.Method == "GET" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		var pending int64
		var age float64
		e := a.store.Pool.QueryRow(ctx, `SELECT count(*),COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(t.created_at)),0) FROM outbox o JOIN transfers t ON t.id=o.id WHERE o.published_at IS NULL`).Scan(&pending, &age)
		if e != nil {
			status = errorResponse(w, e)
			return
		}
		fmt.Fprintf(w, "clearinghouse_http_requests_total %d\nclearinghouse_http_failures_total %d\nclearinghouse_http_duration_seconds_sum %.6f\nclearinghouse_outbox_pending %d\nclearinghouse_outbox_oldest_seconds %.3f\n", a.requests.Load(), a.failures.Load(), float64(a.nanos.Load())/1e9, pending, age)
		return
	}
	auth := r.Header.Get("Authorization")
	tenant, ok := a.tokens[sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))]
	if !strings.HasPrefix(auth, "Bearer ") || !ok {
		status = 401
		respond(w, status, map[string]string{"error": "unauthorized"})
		return
	}
	switch {
	case r.URL.Path == "/v1/transfers" && r.Method == "POST":
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		defer r.Body.Close()
		req, e := decodeTransfer(r.Body)
		if e != nil {
			status = errorResponse(w, ErrInvalid)
			return
		}
		transfer, e := a.store.Transfer(ctx, tenant, r.Header.Get("Idempotency-Key"), req)
		if e != nil {
			status = errorResponse(w, e)
			return
		}
		status = 201
		if transfer.Replayed {
			status = 200
		}
		respond(w, status, transfer)
	case strings.HasPrefix(r.URL.Path, "/v1/accounts/") && r.Method == "GET":
		account, e := a.store.Account(ctx, tenant, strings.TrimPrefix(r.URL.Path, "/v1/accounts/"))
		if e != nil {
			status = errorResponse(w, e)
			return
		}
		respond(w, 200, account)
	case r.URL.Path == "/v1/summary" && r.Method == "GET":
		summary, e := a.store.Summary(ctx, tenant)
		if e != nil {
			status = errorResponse(w, e)
			return
		}
		respond(w, 200, summary)
	case r.URL.Path == "/v1/reconcile" && r.Method == "GET":
		result, e := a.store.Reconcile(ctx, tenant)
		if e != nil {
			status = errorResponse(w, e)
			return
		}
		respond(w, 200, result)
	default:
		status = 404
		respond(w, status, map[string]string{"error": "route not found"})
	}
}
