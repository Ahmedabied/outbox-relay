"""Check the alert rules against a real relay that Prometheus scrapes, then break things and watch the rules.

Prometheus must scrape the relay's /metrics (OUTBOX_METRICS_ADDR) and load alerts.yml, as prometheus.live.yml
does, every 5 s. Three steps, run in order, with your own way to stop Kafka and the relay in between:

    python examples/prometheus/check_live.py healthy    --prometheus http://localhost:9090 --db postgresql://...
    docker stop kafka
    python examples/prometheus/check_live.py kafka-down --prometheus http://localhost:9090 --db postgresql://...
    kill <relay pid>
    python examples/prometheus/check_live.py relay-gone --prometheus http://localhost:9090

healthy: 100 rows go through; all six rules evaluate without error, every metric name in them has a series,
and no alert is pending or firing. kafka-down: one row is written; OutboxStuck must go pending and
OutboxRelayStalled must not (the relay reads on every retry). relay-gone: OutboxNoLeader must go pending.
Pending means the expression is true; the `for:` waits are tested by promtool.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

RULES = Path(__file__).parent / "alerts.yml"
ACTIVE = ("pending", "firing")
TOPIC = "outbox-live-check"  # one topic for both steps, so the producer knows it when Kafka goes


def api(base: str, path: str, **params) -> dict:
    url = f"{base}/api/v1/{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
    with urllib.request.urlopen(url, timeout=10) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(f"{path}: {body}")
    return body["data"]


def value(base: str, expr: str) -> float | None:
    res = api(base, "query", query=expr)["result"]
    return float(res[0]["value"][1]) if res else None


def active(base: str) -> dict[str, str]:
    return {a["labels"]["alertname"]: a["state"] for a in api(base, "alerts")["alerts"] if a["state"] in ACTIVE}


def until(what: str, check, timeout: float, every: float = 1.0):
    deadline = time.monotonic() + timeout
    while True:
        got = check()
        if got:
            return got
        if time.monotonic() > deadline:
            raise TimeoutError(f"{what}: not within {timeout:.0f} s")
        time.sleep(every)


def insert(db: str, table: str, rows: int, topic: str) -> tuple[int, int]:
    import psycopg
    from psycopg import sql

    with psycopg.connect(db, autocommit=True) as conn:
        return conn.execute(sql.SQL(
            """with ins as (insert into {} (topic, msg_key, payload)
                            select %s, 'live-' || g, convert_to('{{"check": ' || g || '}}', 'UTF8')
                              from generate_series(1, %s) g returning id)
               select min(id), max(id) from ins""").format(sql.Identifier(*table.split("."))),
            (topic, rows)).fetchone()


def unsent(db: str, table: str, first: int, last: int) -> int:
    import psycopg
    from psycopg import sql

    with psycopg.connect(db, autocommit=True) as conn:
        return conn.execute(sql.SQL("select count(*) from {} where id between %s and %s and sent_at is null")
                            .format(sql.Identifier(*table.split("."))), (first, last)).fetchone()[0]


def healthy(a) -> list[str]:
    problems = []
    until("Prometheus scrapes a leading relay", lambda: value(a.prometheus, "max(outbox_relay_leading)") == 1, 60)
    before = value(a.prometheus, "max(outbox_relay_published_total)") or 0
    first, last = insert(a.db, a.table, 100, TOPIC)
    until("the relay sends 100 rows", lambda: unsent(a.db, a.table, first, last) == 0, 60, 0.2)
    until("Prometheus sees them", lambda: (value(a.prometheus, "max(outbox_relay_published_total)") or 0)
          >= before + 100, 30)
    time.sleep(11)  # two rule evaluations after that scrape
    rules = [r for g in api(a.prometheus, "rules", type="alert")["groups"] for r in g["rules"]]
    print(f"rules loaded: {len(rules)}, health: {sorted({r['health'] for r in rules})}")
    if len(rules) != 6:
        problems.append(f"expected 6 alert rules, Prometheus has {len(rules)}")
    for r in rules:
        if r["health"] != "ok" or r.get("lastError"):
            problems.append(f"{r['name']}: health {r['health']} {r.get('lastError', '')}")
    text = RULES.read_text()
    names = sorted(set(re.findall(r"\boutbox_relay_[a-z_]+", text)))
    selectors = sorted(set(re.findall(r"\boutbox_relay_[a-z_]+\{[^}]*\}", text)))
    for expr in names + selectors:
        n = value(a.prometheus, f"count({expr})")
        print(f"  {expr:55} {int(n or 0)} series")
        if not n:
            problems.append(f"{expr} has no series in Prometheus")
    on = active(a.prometheus)
    print(f"alerts pending or firing on a healthy relay: {on or 'none'}")
    if on:
        problems.append(f"a healthy relay set off {on}")
    return problems


def kafka_down(a) -> list[str]:
    insert(a.db, a.table, 1, TOPIC)
    t0, seen, worst_read = time.monotonic(), {}, 0.0

    def stuck():
        nonlocal worst_read
        on = active(a.prometheus)
        for name, state in on.items():
            seen.setdefault(name, round(time.monotonic() - t0))
        worst_read = max(worst_read, value(a.prometheus, "time() - max(outbox_relay_last_fetch_timestamp_seconds)") or 0)
        return "OutboxStuck" in seen and time.monotonic() - t0 >= a.watch

    until(f"OutboxStuck goes pending with Kafka down, watched for {a.watch:.0f} s", stuck, a.watch + 240, 2)
    retries = value(a.prometheus, "max(outbox_relay_retries_total)")
    print(f"Kafka down: OutboxStuck pending after {seen['OutboxStuck']} s; alerts seen (s after the insert): {seen};"
          f" longest gap between reads {worst_read:.1f} s; retries {retries:.0f}")
    problems = [] if retries else ["no retries counted while Kafka was down"]
    if "OutboxRelayStalled" in seen:  # the relay reads on every retry, so the read time must stay fresh
        problems.append(f"OutboxRelayStalled went pending {seen['OutboxRelayStalled']} s into the outage")
    return problems


def relay_gone(a) -> list[str]:
    t0 = time.monotonic()
    until("OutboxNoLeader goes pending with the relay stopped", lambda: "OutboxNoLeader" in active(a.prometheus), 60)
    print(f"relay stopped: OutboxNoLeader pending after {time.monotonic() - t0:.0f} s")
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["healthy", "kafka-down", "relay-gone"])
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--db", help="Postgres URL for psycopg (healthy and kafka-down write rows)")
    ap.add_argument("--table", default="outbox")
    ap.add_argument("--watch", type=float, default=150,
                    help="kafka-down: seconds to keep watching, so the relay's backoff reaches its longest")
    a = ap.parse_args()
    if a.step != "relay-gone" and not a.db:
        ap.error("--db is needed for this step")
    problems = {"healthy": healthy, "kafka-down": kafka_down, "relay-gone": relay_gone}[a.step](a)
    for p in problems:
        print("  problem:", p)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
