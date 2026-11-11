CREATE TABLE devices (
  id             uuid PRIMARY KEY,
  name           text NOT NULL,
  api_key_hash   text NOT NULL UNIQUE,
  desired_state  text NOT NULL DEFAULT 'stopped' CHECK (desired_state IN ('started', 'stopped')),
  status         text NOT NULL DEFAULT 'disconnected',
  status_reason  text,
  wa_jid         text,
  wa_lid         text,
  wa_name        text,
  worker_id      text,
  event_seq      bigint NOT NULL DEFAULT 0,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  connected_at   timestamptz
);

-- Baileys authentication state (one creds row + many signal keys per device)
CREATE TABLE auth_creds (
  device_id  uuid PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE,
  creds      jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE auth_keys (
  device_id uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  type      text NOT NULL,
  id        text NOT NULL,
  value     jsonb NOT NULL,
  PRIMARY KEY (device_id, type, id)
);

-- Every message seen by a device (received, sent by the API, sent from the phone)
CREATE TABLE messages (
  id            bigserial PRIMARY KEY,
  device_id     uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  chat_jid      text NOT NULL,
  message_id    text NOT NULL,
  from_me       boolean NOT NULL,
  sender_jid    text,
  type          text NOT NULL,
  status        text,
  ts            timestamptz NOT NULL,
  payload       jsonb NOT NULL,
  raw           jsonb NOT NULL,
  -- true while the message is stored but its event was not emitted yet (e.g. media still downloading)
  pending_event boolean NOT NULL DEFAULT false,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (device_id, chat_jid, message_id, from_me)
);
CREATE INDEX messages_chat_idx ON messages (device_id, chat_jid, ts DESC);
CREATE INDEX messages_msgid_idx ON messages (device_id, message_id);
CREATE INDEX messages_pending_idx ON messages (device_id) WHERE pending_event;

-- Durable send queue. Postgres is the source of truth; Redis only wakes the worker up.
CREATE TABLE outbound_messages (
  id              uuid PRIMARY KEY,
  seq             bigserial NOT NULL,
  device_id       uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  to_jid          text NOT NULL,
  type            text NOT NULL,
  content         jsonb NOT NULL,
  idempotency_key text,
  status          text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'sending', 'sent', 'failed', 'cancelled')),
  ack             text,
  wa_message_id   text,
  attempts        int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  sent_at         timestamptz,
  UNIQUE (device_id, idempotency_key)
);
CREATE INDEX outbound_due_idx ON outbound_messages (device_id, seq) WHERE status IN ('queued', 'sending');
CREATE INDEX outbound_wa_idx ON outbound_messages (device_id, wa_message_id);

-- Event log / transactional outbox. seq is gapless and monotonic per device.
CREATE TABLE events (
  id         uuid PRIMARY KEY,
  device_id  uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  seq        bigint NOT NULL,
  type       text NOT NULL,
  payload    jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (device_id, seq)
);
CREATE INDEX events_created_idx ON events (created_at);

CREATE TABLE webhooks (
  id         uuid PRIMARY KEY,
  device_id  uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  url        text NOT NULL,
  secret     text,
  events     text[] NOT NULL DEFAULT '{}',
  headers    jsonb NOT NULL DEFAULT '{}',
  enabled    boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX webhooks_device_idx ON webhooks (device_id);

CREATE TABLE webhook_deliveries (
  id               uuid PRIMARY KEY,
  event_id         uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  webhook_id       uuid NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
  device_id        uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'failed')),
  attempts         int NOT NULL DEFAULT 0,
  last_status_code int,
  last_error       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  delivered_at     timestamptz
);
CREATE INDEX deliveries_pending_idx ON webhook_deliveries (created_at) WHERE status = 'pending';
CREATE INDEX deliveries_device_idx ON webhook_deliveries (device_id, created_at DESC);
CREATE INDEX deliveries_event_idx ON webhook_deliveries (event_id);
