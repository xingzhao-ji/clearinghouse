// Package clearinghouse implements a local demonstration ledger, not a payment processor.
package clearinghouse

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"regexp"
	"time"
)

//go:embed schema.sql
var schema string

const MaxAmount int64 = 1_000_000_000_000

var (
	ErrInvalid      = errors.New("invalid request")
	ErrNotFound     = errors.New("account not found")
	ErrConflict     = errors.New("idempotency or event conflict")
	ErrFunds        = errors.New("insufficient balance")
	ErrCurrency     = errors.New("currency mismatch")
	ErrCapacity     = errors.New("destination balance limit exceeded")
	identifier      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
)

type Store struct{ Pool *pgxpool.Pool }
type TransferRequest struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount"`
}
type Transfer struct {
	ID       int64  `json:"id"`
	Tenant   string `json:"tenant"`
	From     string `json:"from"`
	To       string `json:"to"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Replayed bool   `json:"replayed,omitempty"`
}
type Account struct {
	ID       string `json:"id"`
	Currency string `json:"currency"`
	Balance  int64  `json:"balance"`
}
type Summary struct {
	Currency  string `json:"currency"`
	Transfers int64  `json:"transfers"`
	Volume    int64  `json:"volume"`
	Pending   int64  `json:"pending"`
}
type Reconciliation struct {
	Currency     string `json:"currency"`
	Mismatches   int64  `json:"mismatches"`
	BalanceTotal int64  `json:"balance_total"`
	JournalNet   int64  `json:"journal_net"`
	Transfers    int64  `json:"transfers"`
}

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, e
	}
	cfg.MaxConns = 24
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	if ns := os.Getenv("PGSCHEMA"); ns != "" {
		if !identifier.MatchString(ns) {
			return nil, ErrInvalid
		}
		cfg.ConnConfig.RuntimeParams["search_path"] = ns
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "3000"
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, e
	}
	return &Store{p}, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(812764)"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, schema); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) CreateAccount(ctx context.Context, tenant, id, currency string, opening int64) error {
	if !identifier.MatchString(tenant) || !identifier.MatchString(id) || !currencyPattern.MatchString(currency) || opening < 0 || opening > MaxAmount {
		return ErrInvalid
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "INSERT INTO tenants(id,currency) VALUES($1,$2) ON CONFLICT DO NOTHING", tenant, currency); e != nil {
		return e
	}
	var tenantCurrency string
	if e = tx.QueryRow(ctx, "SELECT currency FROM tenants WHERE id=$1 FOR KEY SHARE", tenant).Scan(&tenantCurrency); e != nil {
		return e
	}
	if tenantCurrency != currency {
		return ErrCurrency
	}
	if _, e = tx.Exec(ctx, "INSERT INTO accounts(tenant,id,currency,opening,balance) VALUES($1,$2,$3,$4,$4)", tenant, id, currency, opening); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Account(ctx context.Context, tenant, id string) (Account, error) {
	var a Account
	e := s.Pool.QueryRow(ctx, "SELECT id,currency,balance FROM accounts WHERE tenant=$1 AND id=$2", tenant, id).Scan(&a.ID, &a.Currency, &a.Balance)
	if errors.Is(e, pgx.ErrNoRows) {
		e = ErrNotFound
	}
	return a, e
}
func fingerprint(r TransferRequest) string {
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (s *Store) Transfer(ctx context.Context, tenant, key string, r TransferRequest) (Transfer, error) {
	if !identifier.MatchString(tenant) || !identifier.MatchString(key) || !identifier.MatchString(r.From) || !identifier.MatchString(r.To) || r.From == r.To || r.Amount <= 0 || r.Amount > MaxAmount {
		return Transfer{}, ErrInvalid
	}
	for attempt := 0; attempt < 5; attempt++ {
		result, e := s.transferOnce(ctx, tenant, key, r)
		var pe *pgconn.PgError
		if !errors.As(e, &pe) || (pe.Code != "40001" && pe.Code != "40P01") {
			return result, e
		}
		select {
		case <-ctx.Done():
			return Transfer{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 5 * time.Millisecond):
		}
	}
	return Transfer{}, fmt.Errorf("transaction retries exhausted")
}
func (s *Store) transferOnce(ctx context.Context, tenant, key string, r TransferRequest) (Transfer, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return Transfer{}, e
	}
	defer tx.Rollback(ctx)
	// Serialize same-tenant/key requests across API processes; hash collision only causes extra waiting.
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", tenant+":"+key); e != nil {
		return Transfer{}, e
	}
	var old Transfer
	var oldHash string
	e = tx.QueryRow(ctx, "SELECT id,tenant,source,destination,amount,currency,fingerprint FROM transfers WHERE tenant=$1 AND key=$2", tenant, key).Scan(&old.ID, &old.Tenant, &old.From, &old.To, &old.Amount, &old.Currency, &oldHash)
	if e == nil {
		if oldHash != fingerprint(r) {
			return Transfer{}, ErrConflict
		}
		old.Replayed = true
		return old, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return Transfer{}, e
	}
	// A fixed account order avoids AB/BA deadlock. READ COMMITTED sees the current locked balances.
	rows, e := tx.Query(ctx, "SELECT id,currency,balance FROM accounts WHERE tenant=$1 AND id IN ($2,$3) ORDER BY id FOR UPDATE", tenant, r.From, r.To)
	if e != nil {
		return Transfer{}, e
	}
	accts := map[string]Account{}
	for rows.Next() {
		var a Account
		if e = rows.Scan(&a.ID, &a.Currency, &a.Balance); e != nil {
			rows.Close()
			return Transfer{}, e
		}
		accts[a.ID] = a
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return Transfer{}, e
	}
	if len(accts) != 2 {
		return Transfer{}, ErrNotFound
	}
	source, dest := accts[r.From], accts[r.To]
	if source.Currency != dest.Currency {
		return Transfer{}, ErrCurrency
	}
	if source.Balance < r.Amount {
		return Transfer{}, ErrFunds
	}
	if dest.Balance > MaxAmount-r.Amount {
		return Transfer{}, ErrCapacity
	}
	if _, e = tx.Exec(ctx, "UPDATE accounts SET balance=balance+CASE WHEN id=$2 THEN -$4::bigint ELSE $4::bigint END WHERE tenant=$1 AND id IN($2,$3)", tenant, r.From, r.To, r.Amount); e != nil {
		return Transfer{}, e
	}
	result := Transfer{Tenant: tenant, From: r.From, To: r.To, Amount: r.Amount, Currency: source.Currency}
	e = tx.QueryRow(ctx, "INSERT INTO transfers(tenant,key,fingerprint,source,destination,amount,currency) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id", tenant, key, fingerprint(r), r.From, r.To, r.Amount, source.Currency).Scan(&result.ID)
	if e != nil {
		return Transfer{}, e
	}
	payload, _ := json.Marshal(result)
	if _, e = tx.Exec(ctx, "INSERT INTO outbox(id,payload) VALUES($1,$2)", result.ID, payload); e != nil {
		return Transfer{}, e
	}
	if e = tx.Commit(ctx); e != nil {
		return Transfer{}, e
	}
	return result, nil
}
func (s *Store) ApplyEvent(ctx context.Context, event Transfer) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var canonical Transfer
	e = tx.QueryRow(ctx, "SELECT id,tenant,source,destination,amount,currency FROM transfers WHERE id=$1", event.ID).Scan(&canonical.ID, &canonical.Tenant, &canonical.From, &canonical.To, &canonical.Amount, &canonical.Currency)
	if errors.Is(e, pgx.ErrNoRows) {
		return ErrConflict
	}
	if e != nil {
		return e
	}
	event.Replayed = false
	if canonical != event {
		return ErrConflict
	}
	tag, e := tx.Exec(ctx, "INSERT INTO inbox(id) VALUES($1) ON CONFLICT DO NOTHING", event.ID)
	if e != nil {
		return e
	}
	if tag.RowsAffected() == 1 {
		if _, e = tx.Exec(ctx, "INSERT INTO summaries(tenant,transfers,volume) VALUES($1,1,$2) ON CONFLICT(tenant) DO UPDATE SET transfers=summaries.transfers+1,volume=summaries.volume+EXCLUDED.volume", event.Tenant, event.Amount); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) Summary(ctx context.Context, tenant string) (Summary, error) {
	var r Summary
	e := s.Pool.QueryRow(ctx, `SELECT COALESCE((SELECT currency FROM tenants WHERE id=$1),''),COALESCE((SELECT transfers FROM summaries WHERE tenant=$1),0),COALESCE((SELECT volume FROM summaries WHERE tenant=$1),0), (SELECT count(*) FROM transfers t WHERE tenant=$1 AND NOT EXISTS(SELECT 1 FROM inbox i WHERE i.id=t.id))`, tenant).Scan(&r.Currency, &r.Transfers, &r.Volume, &r.Pending)
	return r, e
}
func (s *Store) Reconcile(ctx context.Context, tenant string) (Reconciliation, error) {
	var r Reconciliation
	e := s.Pool.QueryRow(ctx, `WITH deltas AS (SELECT account,sum(delta) AS delta FROM entries WHERE tenant=$1 GROUP BY account)
	 SELECT COALESCE((SELECT currency FROM tenants WHERE id=$1),''),(SELECT count(*) FROM accounts a LEFT JOIN deltas d ON d.account=a.id WHERE a.tenant=$1 AND a.balance<>a.opening+COALESCE(d.delta,0)),
	 (SELECT COALESCE(sum(balance),0) FROM accounts WHERE tenant=$1), (SELECT COALESCE(sum(delta),0) FROM entries WHERE tenant=$1), (SELECT count(*) FROM transfers WHERE tenant=$1)`, tenant).Scan(&r.Currency, &r.Mismatches, &r.BalanceTotal, &r.JournalNet, &r.Transfers)
	return r, e
}
