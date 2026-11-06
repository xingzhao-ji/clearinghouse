package main

import (
	ch "clearinghouse"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		slog.Error("stopped", "error", e)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "api", "init|seed|api|publisher|consumer|reconcile")
	addr := flag.String("addr", "127.0.0.1:8089", "HTTP listen address")
	tenant := flag.String("tenant", "demo", "tenant for seed/reconcile")
	count := flag.Int("accounts", 32, "seed account count")
	balance := flag.Int64("balance", 1000000, "seed opening balance in integer units")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL required")
	}
	s, e := ch.Open(ctx, dbURL)
	if e != nil {
		return e
	}
	defer s.Pool.Close()
	switch *mode {
	case "init":
		if ns := os.Getenv("PGSCHEMA"); ns != "" {
			if _, e = s.Pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{ns}.Sanitize()); e != nil {
				return e
			}
		}
		return s.Migrate(ctx)
	case "seed":
		if *count < 2 || *count > 10000 {
			return fmt.Errorf("accounts must be 2..10000")
		}
		for i := 0; i < *count; i++ {
			if e = s.CreateAccount(ctx, *tenant, fmt.Sprintf("a%d", i), "USD", *balance); e != nil {
				return e
			}
		}
		return nil
	case "reconcile":
		r, e := s.Reconcile(ctx, *tenant)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	case "api":
		tokens := map[string]string{}
		if e = json.Unmarshal([]byte(os.Getenv("API_TOKENS")), &tokens); e != nil || len(tokens) == 0 {
			return fmt.Errorf("API_TOKENS must be a JSON object mapping bearer tokens to tenants")
		}
		for token, tenant := range tokens {
			if len(token) < 16 || tenant == "" {
				return fmt.Errorf("tokens must contain at least 16 characters and map to a tenant")
			}
		}
		server := &http.Server{Addr: *addr, Handler: ch.NewAPI(s, tokens), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 8 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
		go func() {
			<-ctx.Done()
			shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			server.Shutdown(shutdown)
		}()
		slog.Info("api ready", "listen", *addr)
		e = server.ListenAndServe()
		if e == http.ErrServerClosed {
			return nil
		}
		return e
	case "publisher", "consumer":
		url := os.Getenv("NATS_URL")
		if url == "" {
			return fmt.Errorf("NATS_URL required")
		}
		stream := os.Getenv("NATS_STREAM")
		if stream == "" {
			stream = "CLEARINGHOUSE"
		}
		bus, e := ch.ConnectBus(url, stream)
		if e != nil {
			return e
		}
		defer bus.Connection.Close()
		for ctx.Err() == nil {
			batchContext, batchCancel := context.WithTimeout(ctx, 6*time.Second)
			var n int
			if *mode == "publisher" {
				n, e = s.PublishPending(batchContext, 64, bus.Publish)
			} else {
				n, e = bus.Consume(batchContext, s)
			}
			batchCancel()
			if e != nil && ctx.Err() == nil {
				slog.Error("worker retry", "mode", *mode, "error", e)
			}
			if n == 0 || e != nil {
				select {
				case <-ctx.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
}
