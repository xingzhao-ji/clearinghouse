package clearinghouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setup(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Fatal("DATABASE_URL required: integration tests use real PostgreSQL")
	}
	ctx := context.Background()
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		t.Fatal(e)
	}
	admin, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	s := &Store{Pool: pool}
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		pool.Close()
		admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	for _, tenant := range []string{"alpha", "beta"} {
		for _, id := range []string{"alice", "bob"} {
			if e = s.CreateAccount(ctx, tenant, id, "USD", 1000); e != nil {
				t.Fatal(e)
			}
		}
	}
	return s
}
func TestConcurrentIdempotencyAndConflict(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	ids := make(chan int64, 24)
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.Transfer(ctx, "alpha", "one-key", TransferRequest{"alice", "bob", 70})
			if e != nil {
				t.Error(e)
				return
			}
			ids <- r.ID
		}()
	}
	wg.Wait()
	close(ids)
	var first int64
	for id := range ids {
		if first == 0 {
			first = id
		}
		if first != id {
			t.Errorf("duplicate transfer: %d != %d", first, id)
		}
	}
	a, e := s.Account(ctx, "alpha", "alice")
	if e != nil || a.Balance != 930 {
		t.Fatalf("balance=%+v err=%v", a, e)
	}
	if _, e = s.Transfer(ctx, "alpha", "one-key", TransferRequest{"alice", "bob", 71}); !errors.Is(e, ErrConflict) {
		t.Fatalf("conflicting key: %v", e)
	}
	var count int
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&count); e != nil || count != 1 {
		t.Fatalf("outbox count %d %v", count, e)
	}
}
func TestConcurrentSpendNeverOverdraws(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	var success atomic.Int32
	for i := range 30 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := s.Transfer(ctx, "alpha", fmt.Sprint(i), TransferRequest{"alice", "bob", 100})
			if e == nil {
				success.Add(1)
			} else if !errors.Is(e, ErrFunds) {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	if success.Load() != 10 {
		t.Fatalf("successful spends %d", success.Load())
	}
	report, e := s.Reconcile(ctx, "alpha")
	if e != nil || report.Mismatches != 0 || report.BalanceTotal != 2000 || report.JournalNet != 0 {
		t.Fatalf("reconcile %+v %v", report, e)
	}
}
func TestTenantAndInputIsolation(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	if e := s.CreateAccount(ctx, "beta", "private", "USD", 5); e != nil {
		t.Fatal(e)
	}
	for _, r := range []TransferRequest{{"alice", "bob", 0}, {"alice", "bob", -1}, {"alice", "alice", 1}, {"alice", "bob", MaxAmount + 1}} {
		if _, e := s.Transfer(ctx, "alpha", "bad", r); !errors.Is(e, ErrInvalid) {
			t.Fatalf("bad input accepted %+v %v", r, e)
		}
	}
	if _, e := s.Transfer(ctx, "alpha", "cross", TransferRequest{"alice", "private", 1}); !errors.Is(e, ErrNotFound) {
		t.Fatalf("cross tenant: %v", e)
	}
	if _, e := s.Transfer(ctx, "alpha", "shared", TransferRequest{"alice", "bob", 1}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Transfer(ctx, "beta", "shared", TransferRequest{"alice", "bob", 2}); e != nil {
		t.Fatal(e)
	}
	a, _ := s.Account(ctx, "beta", "alice")
	if a.Balance != 998 {
		t.Fatal(a)
	}
	if e := s.CreateAccount(ctx, "alpha", "eur", "EUR", 1); !errors.Is(e, ErrCurrency) {
		t.Fatalf("mixed tenant currency accepted: %v", e)
	}
}
func TestJournalImmutableAndRollback(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	r, e := s.Transfer(ctx, "alpha", "immutable", TransferRequest{"alice", "bob", 3})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Pool.Exec(ctx, "UPDATE transfers SET amount=4 WHERE id=$1", r.ID); e == nil {
		t.Fatal("journal mutable")
	}
	if _, e = s.Pool.Exec(ctx, "DELETE FROM transfers WHERE id=$1", r.ID); e == nil {
		t.Fatal("journal deletable")
	}
	_, e = s.Pool.Exec(ctx, `CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected disk boundary failure'; END $$; CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox();`)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Transfer(ctx, "alpha", "rollback", TransferRequest{"alice", "bob", 9}); e == nil {
		t.Fatal("failure ignored")
	}
	a, _ := s.Account(ctx, "alpha", "alice")
	if a.Balance != 997 {
		t.Fatal("partial commit", a)
	}
}
func TestMaterializationDedupesAndRejectsForgedEvent(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	r, e := s.Transfer(ctx, "alpha", "event", TransferRequest{"alice", "bob", 25})
	if e != nil {
		t.Fatal(e)
	}
	for range 2 {
		if e = s.ApplyEvent(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	summary, e := s.Summary(ctx, "alpha")
	if e != nil || summary.Transfers != 1 || summary.Volume != 25 {
		t.Fatalf("summary %+v %v", summary, e)
	}
	r.Amount = 999
	if e = s.ApplyEvent(ctx, r); !errors.Is(e, ErrConflict) {
		t.Fatalf("forged event: %v", e)
	}
}
func TestHTTPBoundary(t *testing.T) {
	s := setup(t)
	h := NewAPI(s, map[string]string{"alpha-secret": "alpha", "beta-secret": "beta"})
	cases := []struct {
		body, token, key string
		status           int
	}{
		{`{"from":"alice","to":"bob","amount":5}`, "", "k", 401},
		{`{"from":"alice","to":"bob","amount":5,"tenant":"beta"}`, "alpha-secret", "k", 400},
		{`{"from":"alice","to":"bob","amount":5} {}`, "alpha-secret", "k", 400},
		{`{"from":"alice","to":"bob","amount":1.5}`, "alpha-secret", "k", 400},
		{`{"from":"alice","to":"bob","amount":1,"amount":null}`, "alpha-secret", "k", 400},
		{`{"from":"alice","from":"bob","to":"alice","amount":1}`, "alpha-secret", "k", 400},
		{`{"from":"alice","to":"bob","amount":5}`, "alpha-secret", "", 400},
		{`{"from":"alice","to":"bob","amount":5}`, "alpha-secret", "ok", 201},
		{`{"from":"alice","to":"bob","amount":5}`, "alpha-secret", "ok", 200},
		{`{"from":"alice","to":"bob","amount":6}`, "alpha-secret", "ok", 409},
	}
	for _, c := range cases {
		req := httptest.NewRequest("POST", "/v1/transfers", strings.NewReader(c.body))
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Idempotency-Key", c.key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != c.status {
			t.Fatalf("want %d got %d %s", c.status, w.Code, w.Body.String())
		}
	}
	req := httptest.NewRequest("GET", "/v1/accounts/alice", nil)
	req.Header.Set("Authorization", "Bearer beta-secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var a Account
	json.Unmarshal(w.Body.Bytes(), &a)
	if w.Code != http.StatusOK || a.Balance != 1000 {
		t.Fatalf("tenant read %+v %s", a, w.Body)
	}
}

func TestLockTimeoutRollbackCanRetrySameKey(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	lock, e := s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Rollback(ctx)
	if _, e = lock.Exec(ctx, "SELECT id FROM accounts WHERE tenant='alpha' AND id='alice' FOR UPDATE"); e != nil {
		t.Fatal(e)
	}
	bounded, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, e = s.Transfer(bounded, "alpha", "blocked", TransferRequest{"alice", "bob", 50}); e == nil {
		t.Fatal("blocked request exceeded deadline without error")
	}
	if e = lock.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Transfer(ctx, "alpha", "blocked", TransferRequest{"alice", "bob", 50}); e != nil {
		t.Fatal(e)
	}
	a, e := s.Account(ctx, "alpha", "alice")
	if e != nil || a.Balance != 950 {
		t.Fatalf("partial effect or failed retry: %+v %v", a, e)
	}
}
