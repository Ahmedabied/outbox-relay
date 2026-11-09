-- Consumer side: the outbox ids each consumer has already handled. Safe to run again.
create table if not exists outbox_inbox (
    consumer    text        not null,
    outbox_id   bigint      not null,
    received_at timestamptz not null default now(),
    primary key (consumer, outbox_id)
);
