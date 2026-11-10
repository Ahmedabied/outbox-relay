package io.github.ahmedabied.outbox;

import static io.github.ahmedabied.outbox.Fakes.fast;
import static io.github.ahmedabied.outbox.Fakes.ids;
import static io.github.ahmedabied.outbox.Fakes.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

class RelayTest {

    /** Step until the outbox is empty; fail if that takes absurdly long. */
    private static void drain(Relay relay) throws InterruptedException {
        for (int i = 0; i < 10_000; i++) {
            if (relay.step() == Relay.Step.IDLE) {
                return;
            }
        }
        throw new AssertionError("relay never ran dry");
    }

    @Test
    void everyRowGoesOutOnceInIdOrder() throws Exception {
        var db = new Fakes.Db().insert(1_200, 7);
        var kafka = new Fakes.Kafka();
        var relay = new Relay(new Fakes.Store(db), kafka, fast(500));
        drain(relay);
        assertEquals(range(1, 1_200), ids(kafka.visible));
        assertEquals(1_200, db.sent.size());
        assertEquals(List.of(500, 500, 200), kafka.batchSizes);
        assertEquals(1_200, relay.stats().published.sum());
    }

    @Test
    void brokerTroubleIsRetriedAndLeavesNoGapOrRepeat() throws Exception {
        var db = new Fakes.Db().insert(1_000, 7);
        var kafka = new Fakes.Kafka();
        kafka.retriableFailures = 5;
        var relay = new Relay(new Fakes.Store(db), kafka, fast(300));
        drain(relay);
        assertEquals(range(1, 1_000), ids(kafka.visible)); // failed batches were never visible
        assertEquals(5, relay.stats().retries.sum());
    }

    @Test
    void aRowKafkaRefusesStopsTheRelayAfterEverythingBeforeIt() throws Exception {
        var db = new Fakes.Db().insert(500, 7);
        var kafka = new Fakes.Kafka();
        kafka.poison.add(250L);
        var relay = new Relay(new Fakes.Store(db), kafka, fast(500));
        var e = assertThrows(RelayStoppedException.class, () -> drain(relay));
        assertTrue(e.getMessage().startsWith("row 250 cannot be published"), e.getMessage());
        assertEquals(range(1, 249), ids(kafka.visible)); // all before it, nothing after it
        assertEquals(249, db.sent.size());
        // 500 refused, 249 before it published, then the bad row alone three times
        assertEquals(List.of(500, 249, 1, 1, 1), kafka.batchSizes);
    }

    @Test
    void theBadRowIsFoundByHalvingWhenKafkaDoesNotNameIt() throws Exception {
        var db = new Fakes.Db().insert(500, 7);
        var kafka = new Fakes.Kafka();
        kafka.poison.add(250L);
        kafka.namesTheRow = false;
        var relay = new Relay(new Fakes.Store(db), kafka, fast(500));
        var e = assertThrows(RelayStoppedException.class, () -> drain(relay));
        assertTrue(e.getMessage().startsWith("row 250 cannot be published"), e.getMessage());
        assertEquals(range(1, 249), ids(kafka.visible));
    }

    @Test
    void aBrokerOutageWaitsLongerEachTryAndTheFirstPublishStartsOver() throws Exception {
        var db = new Fakes.Db().insert(10, 3);
        var kafka = new Fakes.Kafka();
        kafka.retriableFailures = 8; // the database answers every fetch; only Kafka is down
        List<Long> waits = new ArrayList<>();
        var relay = new Relay(new Fakes.Store(db), kafka, Relay.Settings.defaults(), Clock.systemUTC(),
                w -> waits.add(w.toMillis()));
        drain(relay);
        assertEquals(List.of(100L, 200L, 400L, 800L, 1_600L, 3_200L, 6_400L, 10_000L), waits);
        db.insert(10, 3);
        kafka.retriableFailures = 1;
        drain(relay);
        assertEquals(100L, waits.get(waits.size() - 1)); // a new outage starts at the shortest wait
        assertEquals(range(1, 20), ids(kafka.visible));
    }

    @Test
    void anOutageThatEndsOnAnEmptyOutboxDoesNotSlowTheNextOne() throws Exception {
        var store = new Fakes.Store(new Fakes.Db());
        store.failLeads = 6; // the database is down for six tries, then back, with nothing to send
        List<Long> waits = new ArrayList<>();
        var relay = new Relay[1];
        int[] awaits = {0};
        store.onAwait = () -> {
            if (++awaits[0] == 1) {
                store.failFetches = 1; // later, one unrelated dropped connection
            } else {
                relay[0].stop();
            }
        };
        relay[0] = new Relay(store, new Fakes.Kafka(), Relay.Settings.defaults(), Clock.systemUTC(),
                w -> waits.add(w.toMillis()));
        relay[0].run();
        assertEquals(List.of(100L, 200L, 400L, 800L, 1_600L, 3_200L, 100L), waits);
    }

    @Test
    void aStandbyThatReachesTheDatabaseAgainStartsItsWaitsOver() throws Exception {
        var db = new Fakes.Db().insert(10, 3);
        assertTrue(new Fakes.Store(db).tryLead()); // another relay leads
        var standby = new Fakes.Store(db);
        standby.failLeads = 6;
        List<Long> waits = new ArrayList<>();
        var relay = new Relay[1];
        relay[0] = new Relay(standby, new Fakes.Kafka(), Relay.Settings.defaults(), Clock.systemUTC(), w -> {
            waits.add(w.toMillis());
            if (waits.size() == 7) {
                standby.failLeads = 1; // after one quiet standby wait, one more dropped connection
            } else if (waits.size() == 9) {
                relay[0].stop();
            }
        });
        relay[0].run();
        assertEquals(List.of(100L, 200L, 400L, 800L, 1_600L, 3_200L, 1_000L, 100L, 1_000L), waits);
    }

    @Test
    void troubleBeforeTheLeadIsLoggedOnceNotEveryRetry() throws Exception {
        var db = new Fakes.Db().insert(10, 3);
        var store = new Fakes.Store(db);
        store.failLeads = 5; // say the table was never made: the relay must say so, not retry in silence
        List<String> logged = Collections.synchronizedList(new ArrayList<>());
        Handler h = new Handler() {
            @Override
            public void publish(LogRecord r) {
                logged.add(r.getMessage());
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Logger log = Logger.getLogger(Relay.class.getName());
        log.addHandler(h);
        try {
            var kafka = new Fakes.Kafka();
            drain(new Relay(store, kafka, fast(100)));
            assertEquals(10, kafka.visible.size());
        } finally {
            log.removeHandler(h);
        }
        assertEquals(1, logged.stream().filter(m -> m.contains("does not exist")).count(), logged.toString());
    }

    @Test
    void aLostConnectionBetweenKafkaAndTheMarkRepeatsNothing() throws Exception {
        var db = new Fakes.Db().insert(1_000, 7);
        var kafka = new Fakes.Kafka();
        var store = new Fakes.Store(db);
        store.failMarks = 1; // Kafka has the first batch, then the database connection goes
        var relay = new Relay(store, kafka, fast(300));
        drain(relay);
        assertEquals(range(1, 1_000), ids(kafka.visible)); // the relay remembered the batch and marked it: no repeat
        assertEquals(1, relay.stats().databaseErrors.sum());
        assertEquals(1_000, relay.stats().published.sum());
    }

    @Test
    void aProcessThatDiesBetweenKafkaAndTheMarkRepeatsThatBatchOnly() throws Exception {
        var db = new Fakes.Db().insert(1_000, 7);
        var kafka = new Fakes.Kafka();
        var dying = new Fakes.Store(db);
        dying.failMarks = 1;
        var first = new Relay(dying, kafka, fast(300));
        while (first.step() != Relay.Step.RETRY) {
            // until the first batch is in Kafka and the mark has failed; then this process is gone
        }
        var next = new Relay(new Fakes.Store(db), kafka, fast(300));
        drain(next);
        List<Long> seen = ids(kafka.visible);
        assertEquals(1_300, seen.size()); // that batch twice
        assertEquals(range(1, 1_000), new ArrayList<>(new LinkedHashSet<>(seen))); // dropping repeats by id: in order
    }

    @Test
    void aMarkThatKeepsFailingIsRetriedWithoutPublishingAgain() throws Exception {
        var db = new Fakes.Db().insert(1_000, 7);
        var kafka = new Fakes.Kafka();
        var store = new Fakes.Store(db);
        store.failMarks = 4;
        var relay = new Relay(store, kafka, fast(300));
        drain(relay);
        assertEquals(range(1, 1_000), ids(kafka.visible)); // four failed marks, still one copy of each row
        assertEquals(4, relay.stats().databaseErrors.sum());
    }

    @Test
    void onlyTheLeaderPublishesAndAStandbyTakesOverWhenItGoes() throws Exception {
        var db = new Fakes.Db().insert(100, 3);
        var visible = new ArrayList<OutboxRow>();
        var first = new Fakes.Store(db);
        var second = new Fakes.Store(db);
        var a = new Relay(first, new Fakes.Kafka(visible), fast(40));
        var b = new Relay(second, new Fakes.Kafka(visible), fast(40));
        assertEquals(Relay.Step.FULL, a.step());
        assertEquals(Relay.Step.STANDBY, b.step());
        first.close(); // the leader's connection drops
        db.insert(10, 3);
        drain(b);
        assertTrue(b.leading());
        assertEquals(range(1, 110), ids(visible));
    }

    @Test
    void aFencedRelayStopsWithoutMarkingAnything() {
        var db = new Fakes.Db().insert(10, 3);
        var kafka = new Fakes.Kafka();
        kafka.fenced = true;
        var relay = new Relay(new Fakes.Store(db), kafka, fast(500));
        var e = assertThrows(RelayStoppedException.class, relay::step);
        assertTrue(e.getMessage().contains("same transactional id"));
        assertTrue(db.sent.isEmpty());
    }

    @Test
    void runReturnsWhenStopped() throws Exception {
        var db = new Fakes.Db().insert(50, 3);
        var kafka = new Fakes.Kafka();
        var relay = new Relay(new Fakes.Store(db), kafka, fast(500));
        Thread t = new Thread(() -> {
            try {
                relay.run();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();
        while (relay.stats().published.sum() < 50) {
            Thread.sleep(1);
        }
        relay.stop();
        t.join(2_000);
        assertFalse(t.isAlive());
        assertEquals(range(1, 50), ids(kafka.visible));
    }

    @Test
    void lagIsMeasuredFromInsertToCommit() throws Exception {
        var db = new Fakes.Db().insert(100, 3);
        var relay = new Relay(new Fakes.Store(db), new Fakes.Kafka(), fast(500));
        drain(relay);
        assertTrue(relay.stats().lag(0.99).toMillis() < 5_000);
        assertTrue(relay.stats().toString().startsWith("published 100 in 1 batches"));
    }
}
