-- Per-device behaviour switches (e.g. presenceEvents)
ALTER TABLE devices ADD COLUMN settings jsonb NOT NULL DEFAULT '{}';

-- Current reaction of each person on each message (a new reaction replaces the previous one)
CREATE TABLE message_reactions (
  device_id   uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  chat_jid    text NOT NULL,
  message_id  text NOT NULL,
  sender_jid  text NOT NULL,
  sender_alt  text,
  from_me     boolean NOT NULL,
  emoji       text NOT NULL,
  ts          timestamptz NOT NULL,
  PRIMARY KEY (device_id, chat_jid, message_id, sender_jid)
);
CREATE INDEX message_reactions_msg_idx ON message_reactions (device_id, message_id);

-- Chats whose presence (typing, online) the device follows; WhatsApp forgets these on every reconnect
CREATE TABLE presence_subscriptions (
  device_id  uuid NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  jid        text NOT NULL,
  touched_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (device_id, jid)
);
