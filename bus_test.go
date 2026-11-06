package clearinghouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"os"
	"testing"
	"time"
)

func TestRealBrokerAckLossAndConsumerRedelivery(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Fatal("NATS_URL required")
	}
	bus, e := ConnectBus(url, fmt.Sprintf("test_%d", time.Now().UnixNano()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { bus.JS.DeleteStream(bus.Stream); bus.Connection.Close() })
	result, e := s.Transfer(ctx, "alpha", "delivery", TransferRequest{"alice", "bob", 20})
	if e != nil {
		t.Fatal(e)
	}
	// A broker accepted the event but the publisher lost its acknowledgement before its DB commit.
	_, e = s.PublishPending(ctx, 1, func(ctx context.Context, payload []byte) error {
		if e := bus.Publish(ctx, payload); e != nil {
			return e
		}
		return errors.New("simulated lost publish ack")
	})
	if e == nil {
		t.Fatal("ack loss ignored")
	}
	var published bool
	s.Pool.QueryRow(ctx, "SELECT published_at IS NOT NULL FROM outbox WHERE id=$1", result.ID).Scan(&published)
	if published {
		t.Fatal("failed publish marked committed")
	}
	n, e := s.PublishPending(ctx, 10, bus.Publish)
	if e != nil || n != 1 {
		t.Fatalf("retry %d %v", n, e)
	}
	messages, e := bus.Subscription.Fetch(1, nats.MaxWait(2*time.Second))
	if e != nil {
		t.Fatal(e)
	}
	var event Transfer
	if e = json.Unmarshal(messages[0].Data, &event); e != nil {
		t.Fatal(e)
	}
	if e = s.ApplyEvent(ctx, event); e != nil {
		t.Fatal(e)
	} // commit succeeded, consumer dies before Ack.
	deadline := time.Now().Add(5 * time.Second)
	var received int
	for time.Now().Before(deadline) {
		n, e = bus.Consume(ctx, s)
		if e != nil {
			t.Fatal(e)
		}
		received += n
		if received > 0 {
			break
		}
	}
	if received != 1 {
		t.Fatalf("expected redelivery, got %d", received)
	}
	summary, e := s.Summary(ctx, "alpha")
	if e != nil || summary.Transfers != 1 || summary.Volume != 20 || summary.Pending != 0 {
		t.Fatalf("effects duplicated or missing %+v %v", summary, e)
	}
}

func TestPoisonEventsAreQuarantinedWithoutBlockingValidDelivery(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	bus, e := ConnectBus(os.Getenv("NATS_URL"), fmt.Sprintf("test_%d", time.Now().UnixNano()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { bus.JS.DeleteStream(bus.Stream); bus.Connection.Close() })
	for _, payload := range []string{"{broken", `{"id":999,"tenant":"alpha","from":"alice","to":"bob","amount":1,"currency":"USD"}`} {
		if _, e = bus.JS.Publish(bus.Subject, []byte(payload)); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = s.Transfer(ctx, "alpha", "after-poison", TransferRequest{"alice", "bob", 7}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.PublishPending(ctx, 10, bus.Publish); e != nil {
		t.Fatal(e)
	}
	n, e := bus.Consume(ctx, s)
	if e != nil || n != 1 {
		t.Fatalf("valid event blocked: %d %v", n, e)
	}
	var quarantined int
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FROM dead_letters").Scan(&quarantined); e != nil || quarantined != 2 {
		t.Fatalf("quarantine %d %v", quarantined, e)
	}
	sum, e := s.Summary(ctx, "alpha")
	if e != nil || sum.Transfers != 1 || sum.Volume != 7 {
		t.Fatalf("summary %+v %v", sum, e)
	}
}

func TestPublishTimesOutWhenBrokerReplyIsLost(t *testing.T) {
	nc, e := nats.Connect(os.Getenv("NATS_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer nc.Close()
	subject := fmt.Sprintf("blackhole_%d", time.Now().UnixNano())
	sub, e := nc.Subscribe(subject, func(*nats.Msg) {})
	if e != nil {
		t.Fatal(e)
	}
	defer sub.Unsubscribe()
	if e = nc.Flush(); e != nil {
		t.Fatal(e)
	}
	js, e := nc.JetStream(nats.MaxWait(2 * time.Second))
	if e != nil {
		t.Fatal(e)
	}
	bus := &Bus{Connection: nc, JS: js, Subject: subject}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- bus.Publish(ctx, []byte(`{"id":1}`)) }()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("missing ack reported success")
		}
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatal("publish with no parent deadline waited indefinitely for reply")
	}
}
