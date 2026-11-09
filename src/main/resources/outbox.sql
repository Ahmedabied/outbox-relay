-- outbox-relay schema. Safe to run again.
-- The app inserts into outbox in the same transaction as its own data; the relay does the rest.

create table if not exists outbox (
    id         bigint generated always as identity primary key,
    topic      text        not null,
    msg_key    text,
    payload    bytea       not null,
    headers    jsonb       not null default '{}',
    created_at timestamptz not null default clock_timestamp(),
    sent_at    timestamptz
);

-- The relay reads only unsent rows, in id order: this index stays as small as the backlog.
create index if not exists outbox_unsent on outbox (id) where sent_at is null;

-- One notification per inserting statement, delivered at commit, wakes the relay at once.
create or replace function outbox_notify() returns trigger language plpgsql as $$
begin
    perform pg_notify('outbox_relay', '');
    return null;
end
$$;

drop trigger if exists outbox_notify on outbox;
create trigger outbox_notify after insert on outbox for each statement execute function outbox_notify();
