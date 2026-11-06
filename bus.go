package clearinghouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"time"
)

type Bus struct {
	Connection   *nats.Conn
	JS           nats.JetStreamContext
	Subscription *nats.Subscription
	Stream       string
	Subject      string
}

func ConnectBus(url, stream string) (*Bus, error) {
	if !identifier.MatchString(stream) {
		return nil, ErrInvalid
	}
	nc, e := nats.Connect(url, nats.Name("clearinghouse"), nats.Timeout(2*time.Second), nats.MaxReconnects(-1))
	if e != nil {
		return nil, e
	}
	js, e := nc.JetStream(nats.MaxWait(2 * time.Second))
	if e != nil {
		nc.Close()
		return nil, e
	}
	subject := stream + ".transfers"
	_, e = js.StreamInfo(stream)
	if errors.Is(e, nats.ErrStreamNotFound) {
		_, e = js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{subject}, Storage: nats.FileStorage, Retention: nats.LimitsPolicy, MaxBytes: 256 * 1024 * 1024, Discard: nats.DiscardNew, Duplicates: 2 * time.Minute})
	}
	if e != nil {
		nc.Close()
		return nil, e
	}
	sub, e := js.PullSubscribe(subject, "materializer", nats.BindStream(stream), nats.ManualAck(), nats.AckExplicit(), nats.AckWait(time.Second), nats.MaxAckPending(256))
	if e != nil {
		nc.Close()
		return nil, e
	}
	return &Bus{nc, js, sub, stream, subject}, nil
}
func (b *Bus) Publish(ctx context.Context, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var event Transfer
	if e := json.Unmarshal(payload, &event); e != nil {
		return e
	}
	msg := nats.NewMsg(b.Subject)
	msg.Data = payload
	msg.Header.Set(nats.MsgIdHdr, fmt.Sprint(event.ID))
	_, e := b.JS.PublishMsg(msg, nats.Context(ctx))
	return e
}

// PublishPending holds only outbox row locks across the broker operation. It never holds account locks.
// A lost publish acknowledgement leaves the row pending. Delivery can repeat; inbox makes effects idempotent.
func (s *Store) PublishPending(ctx context.Context, limit int, publish func(context.Context, []byte) error) (int, error) {
	if limit < 1 || limit > 100 {
		return 0, ErrInvalid
	}
	n := 0
	for ; n < limit; n++ {
		tx, e := s.Pool.Begin(ctx)
		if e != nil {
			return n, e
		}
		var id int64
		var payload []byte
		e = tx.QueryRow(ctx, "SELECT id,payload FROM outbox WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1").Scan(&id, &payload)
		if errors.Is(e, pgx.ErrNoRows) {
			tx.Rollback(ctx)
			return n, nil
		}
		if e != nil {
			tx.Rollback(ctx)
			return n, e
		}
		if e = publish(ctx, payload); e != nil {
			tx.Rollback(ctx)
			return n, e
		}
		if _, e = tx.Exec(ctx, "UPDATE outbox SET published_at=clock_timestamp() WHERE id=$1", id); e != nil {
			tx.Rollback(ctx)
			return n, e
		}
		if e = tx.Commit(ctx); e != nil {
			return n, e
		}
	}
	return n, nil
}
func (b *Bus) Consume(ctx context.Context, s *Store) (int, error) {
	messages, e := b.Subscription.Fetch(64, nats.MaxWait(250*time.Millisecond))
	if errors.Is(e, nats.ErrTimeout) {
		return 0, nil
	}
	if e != nil {
		return 0, e
	}
	count := 0
	for _, msg := range messages {
		var event Transfer
		decodeErr := json.Unmarshal(msg.Data, &event)
		if decodeErr == nil {
			e = s.ApplyEvent(ctx, event)
		} else {
			e = ErrInvalid
		}
		quarantined := errors.Is(e, ErrConflict) || errors.Is(e, ErrInvalid)
		if quarantined {
			meta, metaErr := msg.Metadata()
			if metaErr != nil {
				return count, metaErr
			}
			hash := sha256.Sum256(msg.Data)
			prefix := msg.Data
			if len(prefix) > 4096 {
				prefix = prefix[:4096]
			}
			_, e = s.Pool.Exec(ctx, "INSERT INTO dead_letters(stream,sequence,payload_sha256,payload_prefix,reason) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", meta.Stream, meta.Sequence.Stream, hex.EncodeToString(hash[:]), prefix, e.Error())
		}
		if e != nil {
			return count, e
		}
		ackContext, cancel := context.WithTimeout(ctx, 2*time.Second)
		e = msg.AckSync(nats.Context(ackContext))
		cancel()
		if e != nil {
			return count, e
		}
		if !quarantined {
			count++
		}
	}
	return count, nil
}
