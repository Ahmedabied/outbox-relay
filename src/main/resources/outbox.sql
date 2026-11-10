-- outbox-relay schema. Safe to run again, on a busy table too.
-- The app inserts into the outbox table in the same transaction as its own data; the relay does the rest.
-- {table} and the other names in braces are filled in from OUTBOX_TABLE (default outbox), already quoted.
--
-- A run that finds everything in place takes no lock on the table. That matters because a schema change that has to
-- wait for a lock makes every later insert wait behind it: with one long transaction open on the table, a plain
-- "create index if not exists" or "drop trigger if exists" stalls the application until that transaction ends.
-- When the table does have to change (an upgrade from an older version), the wait is capped: after 3 s the run
-- gives up and nothing has changed. install() runs this file in one transaction, which "set local" needs.

set local lock_timeout = '3s';

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
-- Looked up first: "create index if not exists" locks the table against writes before it checks the name.
do $$
begin
    if to_regclass({index_literal}) is null then
        create index {index} on {table} (id) where sent_at is null;
    end if;
end
$$;

-- One notification per inserting statement, delivered at commit, wakes the relay at once.
create or replace function {function}() returns trigger language plpgsql as $$
begin
    perform pg_notify({channel_literal}, '');
    return null;
end
$$;

-- The trigger has had this one form since the first version, so one that exists is left alone: dropping and making
-- it again would take ACCESS EXCLUSIVE on the table for nothing. What it does lives in the function above.
do $$
begin
    if not exists (select 1 from pg_trigger
                    where tgrelid = {table_literal}::regclass and tgname = {trigger_literal} and not tgisinternal) then
        create trigger {trigger} after insert on {table} for each statement execute function {function}();
    end if;
end
$$;
