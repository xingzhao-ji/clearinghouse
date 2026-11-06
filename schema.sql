CREATE TABLE IF NOT EXISTS tenants (
 id text PRIMARY KEY, currency text NOT NULL CHECK(currency ~ '^[A-Z]{3}$'), UNIQUE(id,currency)
);
CREATE TABLE IF NOT EXISTS accounts (
 tenant text NOT NULL, id text NOT NULL, currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
 opening bigint NOT NULL CHECK(opening BETWEEN 0 AND 1000000000000),
 balance bigint NOT NULL CHECK(balance BETWEEN 0 AND 1000000000000),
 PRIMARY KEY(tenant,id)
);
DO $$ BEGIN
 IF EXISTS(SELECT tenant FROM accounts GROUP BY tenant HAVING count(DISTINCT currency)>1) THEN
  RAISE EXCEPTION 'migration refuses mixed-currency tenant';
 END IF;
END $$;
INSERT INTO tenants(id,currency) SELECT tenant,min(currency) FROM accounts GROUP BY tenant ON CONFLICT DO NOTHING;
DO $$ BEGIN
 ALTER TABLE accounts ADD CONSTRAINT tenant_currency_fk FOREIGN KEY(tenant,currency) REFERENCES tenants(id,currency);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE TABLE IF NOT EXISTS transfers (
 id bigserial PRIMARY KEY, tenant text NOT NULL, key text NOT NULL, fingerprint text NOT NULL,
 source text NOT NULL, destination text NOT NULL, amount bigint NOT NULL CHECK(amount>0 AND amount<=1000000000000),
 currency text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(tenant,key), CHECK(source<>destination),
 FOREIGN KEY(tenant,source) REFERENCES accounts(tenant,id),
 FOREIGN KEY(tenant,destination) REFERENCES accounts(tenant,id)
);
CREATE TABLE IF NOT EXISTS outbox (
 id bigint PRIMARY KEY REFERENCES transfers(id), payload jsonb NOT NULL, published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS inbox (id bigint PRIMARY KEY REFERENCES transfers(id), received_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS dead_letters (
 stream text NOT NULL, sequence bigint NOT NULL, payload_sha256 text NOT NULL,
 payload_prefix bytea NOT NULL, reason text NOT NULL, received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(stream,sequence)
);
CREATE TABLE IF NOT EXISTS summaries (tenant text PRIMARY KEY, transfers bigint NOT NULL DEFAULT 0, volume numeric(30,0) NOT NULL DEFAULT 0);
CREATE OR REPLACE FUNCTION immutable_journal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'transfers are immutable; use a compensating transfer'; END $$;
DROP TRIGGER IF EXISTS immutable_transfers ON transfers;
CREATE TRIGGER immutable_transfers BEFORE UPDATE OR DELETE ON transfers FOR EACH ROW EXECUTE FUNCTION immutable_journal();
CREATE OR REPLACE VIEW entries AS
 SELECT id,tenant,source AS account,-amount AS delta FROM transfers
 UNION ALL SELECT id,tenant,destination AS account,amount AS delta FROM transfers;
