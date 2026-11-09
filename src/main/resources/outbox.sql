-- outbox-relay schema. Safe to run again.
-- The app inserts into the outbox table in the same transaction as its own data; the relay does the rest.
-- {table} and the other names in braces are filled in from OUTBOX_TABLE (default outbox), already quoted.

create table if not exists {table} (
    id         bigint generated always as identity primary key,
    topic      text        not null,
    msg_key    text,
    payload    bytea       not null,
    headers    jsonb       not null default '{}',
    created_at timestamptz not null default clock_timestamp(),
    sent_at    timestamptz,
    -- Headers the relay could not send as they are. Unsent rows only, so a row that breaks it can still be marked
    -- sent (the relay reads its headers as none).
    constraint {check}
        check (sent_at is not null or (jsonb_typeof(headers) = 'object' and not headers ? 'outbox-id'))
);

-- A table made by an older version gets the check, or has its first form replaced: that one held for marking a row
-- sent too, so one old row with bad headers was published again and again. Old rows are not scanned (not valid).
do $$
begin
    if not exists (select 1 from pg_constraint
                    where conname = {check_literal} and conrelid = {table_literal}::regclass
                      and pg_get_constraintdef(oid) like '%sent_at IS NOT NULL%') then
        alter table {table} drop constraint if exists {check};
        alter table {table} add constraint {check}
            check (sent_at is not null or (jsonb_typeof(headers) = 'object' and not headers ? 'outbox-id')) not valid;
    end if;
end
$$;

-- The relay reads only unsent rows, in id order: this index stays as small as the backlog.
create index if not exists {index} on {table} (id) where sent_at is null;

-- One notification per inserting statement, delivered at commit, wakes the relay at once.
create or replace function {function}() returns trigger language plpgsql as $$
begin
    perform pg_notify({channel_literal}, '');
    return null;
end
$$;

drop trigger if exists {trigger} on {table};
create trigger {trigger} after insert on {table} for each statement execute function {function}();
