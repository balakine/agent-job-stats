package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.ExtensionList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Keeps the recent history of Jenkins activity in memory, numbered.
 *
 * <p>Numbering is what lets a client tell a quiet Jenkins from a connection
 * that dropped and missed something: with a sequence number on every event and
 * a bounded backlog, a reconnect replays the difference instead of leaving a
 * silent hole.
 *
 * <p>The log holds {@link Event} objects rather than rendered text, because each
 * one carries the job that decides who may see it. The filtering happens per
 * reader, in {@link StreamRootAction}.
 */
@Extension
public class EventLog {

    /** Events kept for replay; raise it for an instance with a busy queue. */
    static final int CAPACITY = Integer.getInteger(
            EventLog.class.getName() + ".capacity", 2000);

    /**
     * How many events a single slow reader may fall behind before it is told it
     * has lost history rather than being allowed to consume memory.
     */
    static final int READER_BACKLOG = Integer.getInteger(
            EventLog.class.getName() + ".readerBacklog", 1000);

    /** One event, with the sequence number a client resumes from. */
    static final class Entry {
        final long seq;
        final Event event;

        Entry(long seq, Event event) {
            this.seq = seq;
            this.event = event;
        }
    }

    /** A live reader. Its queue is drained by the request thread serving it. */
    static final class Reader {
        private final LinkedBlockingQueue<Entry> queue = new LinkedBlockingQueue<>(READER_BACKLOG);
        private volatile boolean lagged;

        void offer(Entry entry) {
            if (!queue.offer(entry)) {
                // The reader cannot keep up. Dropping the event quietly would
                // be the one thing this whole design exists to avoid, so the
                // reader is told instead.
                lagged = true;
            }
        }

        Entry poll(long timeout, TimeUnit unit) throws InterruptedException {
            return queue.poll(timeout, unit);
        }

        boolean hasLagged() {
            return lagged;
        }
    }

    /**
     * Identifies this log's numbering. Sequence numbers live in memory, so a
     * Jenkins restart starts them over; a client holding a cursor from before
     * the restart would otherwise ask for events "after 400" and silently skip
     * the new 1..400.
     */
    private final String epoch = UUID.randomUUID().toString();

    private final ArrayDeque<Entry> history = new ArrayDeque<>();
    private final CopyOnWriteArrayList<Reader> readers = new CopyOnWriteArrayList<>();
    private long nextSeq = 1;

    public static EventLog get() {
        return ExtensionList.lookupSingleton(EventLog.class);
    }

    /**
     * Records an event and hands it to every attached reader. Called from
     * Jenkins's own listener threads, so it holds the lock only long enough to
     * number the event.
     */
    void record(Event event) {
        Entry entry;
        synchronized (this) {
            entry = new Entry(nextSeq++, event);
            history.addLast(entry);
            while (history.size() > CAPACITY) {
                history.removeFirst();
            }
        }
        for (Reader reader : readers) {
            reader.offer(entry);
        }
    }

    /** Identifies this numbering; changes whenever Jenkins restarts. */
    String epoch() {
        return epoch;
    }

    /** Sequence number of the newest event, or 0 when nothing has happened. */
    synchronized long latestSeq() {
        return nextSeq - 1;
    }

    /** Sequence number of the oldest event still replayable. */
    synchronized long oldestSeq() {
        Entry first = history.peekFirst();
        return first == null ? 0 : first.seq;
    }

    /**
     * Registers a reader and returns everything after {@code since} that is
     * still held, so nothing published between the two calls can slip through
     * the gap between replay and live delivery.
     */
    synchronized List<Entry> attach(Reader reader, long since) {
        readers.add(reader);
        List<Entry> backlog = new ArrayList<>();
        for (Entry entry : history) {
            if (entry.seq > since) {
                backlog.add(entry);
            }
        }
        return backlog;
    }

    void detach(Reader reader) {
        readers.remove(reader);
    }
}
