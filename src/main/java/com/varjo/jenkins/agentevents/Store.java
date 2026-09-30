package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.ExtensionList;
import hudson.init.Terminator;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import jenkins.model.Jenkins;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

/**
 * The plugin's only record of what happened: an SQLite database at
 * {@code JENKINS_HOME/agent-events/events.db}.
 *
 * <p>Listeners write rows inside their callback, and the stream reads the same
 * rows. Each row carries the sequence numbers of its changes: {@code started_seq}
 * and {@code ended_seq} on a build, {@code allocated_seq} and
 * {@code released_seq} on an allocation. The events after a cursor are a query
 * across those columns.
 *
 * <p>Jobs and nodes get a UUID when first seen and keep it through renames and
 * moves; Jenkins has no stable identity for either.
 *
 * <p>Writes are synchronous, on the listener's thread, serialized on one
 * connection. Readers use their own connections, which WAL runs concurrently
 * with the writer.
 *
 * <p>The database is opened when Jenkins first looks this extension up. If that
 * fails, the extension is not loaded, the cause is in the system log, and each
 * listener logs a warning.
 */
@Extension
public class Store {

    private static final Logger LOGGER = Logger.getLogger(Store.class.getName());

    /**
     * The schema version this build expects. Version {@code n} is reached by
     * running {@code schema/n.sql} on version {@code n - 1}, and the version a
     * database is at is its {@code PRAGMA user_version}. A script that has
     * shipped is never edited: a change is a new script and a bump here.
     */
    private static final int SCHEMA_VERSION = 2;

    /** One change, as the stream shows it: a build or an allocation at one sequence number. */
    record Row(
            long seq,
            String event,
            long at,
            String jobId,
            String jobName,
            boolean jobDeleted,
            int build,
            String result,
            String nodeId,
            String nodeName,
            boolean nodeDeleted,
            Integer executor,
            String task,
            String outcome,
            Long durationMs,
            String problem) {}

    /** Each branch reads one sequence column; the outer query merges them in order. */
    private static final String EVENTS_AFTER = """
        SELECT e.seq, e.event, e.at, j.id, j.full_name, j.deleted_at IS NOT NULL, e.number,
               e.result, n.id, n.name, n.deleted_at IS NOT NULL, e.executor, e.task,
               e.outcome, e.duration_ms, e.problem
        FROM (
            SELECT * FROM (
                SELECT started_seq AS seq, 'build_started' AS event, started_at AS at, job_id, number,
                       NULL AS result, NULL AS node_id, NULL AS executor, NULL AS task,
                       NULL AS outcome, NULL AS duration_ms, NULL AS problem
                FROM builds WHERE started_seq > ?1 ORDER BY started_seq LIMIT ?2)
            UNION ALL
            SELECT * FROM (
                SELECT ended_seq, 'build_ended', ended_at, job_id, number, result,
                       NULL, NULL, NULL, NULL, NULL, NULL
                FROM builds WHERE ended_seq > ?1 ORDER BY ended_seq LIMIT ?2)
            UNION ALL
            SELECT * FROM (
                SELECT a.allocated_seq, 'executor_allocated', a.allocated_at, b.job_id, b.number,
                       NULL, a.node_id, a.executor, a.task, NULL, NULL, NULL
                FROM allocations a JOIN builds b ON b.id = a.build_id
                WHERE a.allocated_seq > ?1 ORDER BY a.allocated_seq LIMIT ?2)
            UNION ALL
            SELECT * FROM (
                SELECT a.released_seq, 'executor_released', a.released_at, b.job_id, b.number,
                       NULL, a.node_id, a.executor, a.task, a.outcome, a.released_at - a.allocated_at, a.problem
                FROM allocations a JOIN builds b ON b.id = a.build_id
                WHERE a.released_seq > ?1 ORDER BY a.released_seq LIMIT ?2)
        ) e
        JOIN jobs j ON j.id = e.job_id
        LEFT JOIN nodes n ON n.id = e.node_id
        ORDER BY e.seq
        LIMIT ?2
        """;

    private static final String OLDEST_SEQ = """
        SELECT MIN(seq) FROM (
            SELECT MIN(started_seq) AS seq FROM builds
            UNION ALL SELECT MIN(ended_seq) FROM builds
            UNION ALL SELECT MIN(allocated_seq) FROM allocations
            UNION ALL SELECT MIN(released_seq) FROM allocations)
        """;

    private final SQLiteDataSource source;
    private final Connection writer;
    private final String epoch;

    /**
     * The newest committed sequence number, mirrored from the {@code sequence}
     * table so waiting readers can be woken without querying.
     */
    private volatile long latest;

    private final Object newer = new Object();

    public Store() throws SQLException {
        File dir = new File(Jenkins.get().getRootDir(), "agent-events");
        if (!dir.isDirectory() && !dir.mkdirs()) {
            throw new SQLException("cannot create " + dir);
        }
        SQLiteConfig config = new SQLiteConfig();
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        // In WAL mode, NORMAL skips the fsync per commit: a power loss can drop
        // the last commits but does not corrupt the file.
        config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
        config.enforceForeignKeys(true);
        config.setBusyTimeout(5000);
        source = new SQLiteDataSource(config);
        source.setUrl("jdbc:sqlite:" + new File(dir, "events.db").getAbsolutePath());

        writer = source.getConnection();
        writer.setAutoCommit(false);
        try {
            epoch = migrate(writer);
            writer.commit();
        } catch (SQLException e) {
            writer.close();
            throw e;
        }
    }

    public static Store get() {
        return ExtensionList.lookupSingleton(Store.class);
    }

    /** Flushes the write-ahead log and closes the database when Jenkins stops. */
    @Terminator
    public static void shutdown() throws SQLException {
        get().close();
    }

    // ---------------------------------------------------------------- writes

    void buildStarted(String job, int number, long at) throws SQLException {
        write(c -> {
            long build = buildId(c, jobId(c, job), number);
            long seq = nextSeq(c);
            execute(c, "UPDATE builds SET started_at = ?, started_seq = ? WHERE id = ?", at, seq, build);
            return seq;
        });
    }

    void buildEnded(String job, int number, long at, String result) throws SQLException {
        write(c -> {
            long build = buildId(c, jobId(c, job), number);
            long seq = nextSeq(c);
            execute(c, "UPDATE builds SET ended_at = ?, ended_seq = ?, result = ? WHERE id = ?",
                    at, seq, result, build);
            return seq;
        });
    }

    void executorAllocated(String job, int number, String node, int executor, String task, long at)
            throws SQLException {
        write(c -> {
            long build = buildId(c, jobId(c, job), number);
            String nodeId = nodeId(c, node);
            long seq = nextSeq(c);
            execute(c, "INSERT INTO allocations (build_id, node_id, executor, task, allocated_at, allocated_seq)"
                    + " VALUES (?, ?, ?, ?, ?, ?)", build, nodeId, executor, task, at, seq);
            return seq;
        });
    }

    void executorReleased(String job, int number, String node, int executor, long at, String outcome, String problem)
            throws SQLException {
        write(c -> {
            Long allocation = queryLong(c, """
                    SELECT a.id FROM allocations a
                    JOIN builds b ON b.id = a.build_id
                    JOIN jobs j ON j.id = b.job_id
                    JOIN nodes n ON n.id = a.node_id
                    WHERE j.full_name = ? AND j.deleted_at IS NULL AND b.number = ?
                      AND n.name = ? AND n.deleted_at IS NULL AND a.executor = ?
                      AND a.released_at IS NULL
                    """, job, number, node, executor);
            if (allocation == null) {
                // Taken before the plugin was running; nothing to close.
                return 0;
            }
            long seq = nextSeq(c);
            execute(c, "UPDATE allocations SET released_at = ?, released_seq = ?, outcome = ?, problem = ?"
                    + " WHERE id = ?", at, seq, outcome, problem, allocation);
            return seq;
        });
    }

    /** Follows a job to a new full name. Jenkins reports each item inside a moved folder separately. */
    void itemMoved(String from, String to, long at) throws SQLException {
        write(c -> {
            String id = queryString(c, "SELECT id FROM jobs WHERE full_name = ? AND deleted_at IS NULL", from);
            if (id != null) {
                // A live row already holding the new name is stale.
                execute(c, "UPDATE jobs SET deleted_at = ? WHERE full_name = ? AND deleted_at IS NULL AND id <> ?",
                        at, to, id);
                execute(c, "UPDATE jobs SET full_name = ? WHERE id = ?", to, id);
            }
            return 0;
        });
    }

    /** Marks a job as gone; history stays. Jenkins deletes a folder's contents one item at a time. */
    void itemDeleted(String fullName, long at) throws SQLException {
        write(c -> {
            execute(c, "UPDATE jobs SET deleted_at = ? WHERE full_name = ? AND deleted_at IS NULL", at, fullName);
            return 0;
        });
    }

    void nodeRenamed(String from, String to, long at) throws SQLException {
        write(c -> {
            String id = queryString(c, "SELECT id FROM nodes WHERE name = ? AND deleted_at IS NULL", from);
            if (id != null) {
                execute(c, "UPDATE nodes SET deleted_at = ? WHERE name = ? AND deleted_at IS NULL AND id <> ?",
                        at, to, id);
                execute(c, "UPDATE nodes SET name = ? WHERE id = ?", to, id);
            }
            return 0;
        });
    }

    void nodeDeleted(String name, long at) throws SQLException {
        write(c -> {
            execute(c, "UPDATE nodes SET deleted_at = ? WHERE name = ? AND deleted_at IS NULL", at, name);
            return 0;
        });
    }

    /**
     * Deletes builds that finished before {@code cutoff}, and their
     * allocations. Jobs and nodes are kept.
     */
    int prune(long cutoff) throws SQLException {
        int[] deleted = new int[1];
        write(c -> {
            deleted[0] = execute(c, "DELETE FROM builds WHERE COALESCE(ended_at, started_at, 0) < ?", cutoff);
            return 0;
        });
        return deleted[0];
    }

    // ----------------------------------------------------------------- reads

    /** A connection for one stream; WAL lets it run beside the writer. */
    Connection openReader() throws SQLException {
        return source.getConnection();
    }

    String epoch() {
        return epoch;
    }

    long latestSeq() {
        return latest;
    }

    /** The oldest sequence number still replayable, or 0 when there is none. */
    long oldestSeq(Connection reader) throws SQLException {
        Long oldest = queryLong(reader, OLDEST_SEQ);
        return oldest == null ? 0 : oldest;
    }

    List<Row> eventsAfter(Connection reader, long since, int limit) throws SQLException {
        List<Row> rows = new ArrayList<>();
        try (PreparedStatement s = reader.prepareStatement(EVENTS_AFTER)) {
            s.setLong(1, since);
            s.setInt(2, limit);
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    rows.add(new Row(
                            r.getLong(1), r.getString(2), r.getLong(3), r.getString(4), r.getString(5),
                            r.getBoolean(6), r.getInt(7), r.getString(8), r.getString(9), r.getString(10),
                            r.getBoolean(11), (Integer) r.getObject(12), r.getString(13), r.getString(14),
                            r.getObject(15) == null ? null : r.getLong(15), r.getString(16)));
                }
            }
        }
        return rows;
    }

    /** Waits until something newer than {@code seq} is committed, or the timeout passes. */
    boolean awaitNewer(long seq, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (newer) {
            while (latest <= seq) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(newer, left);
            }
        }
        return true;
    }

    // -------------------------------------------------------------- plumbing

    @FunctionalInterface
    private interface Transaction {
        /** Returns the sequence number it consumed, or 0 when it changed nothing a reader sees. */
        long run(Connection c) throws SQLException;
    }

    private synchronized void write(Transaction tx) throws SQLException {
        long seq;
        try {
            seq = tx.run(writer);
            writer.commit();
        } catch (SQLException | RuntimeException e) {
            writer.rollback();
            throw e;
        }
        if (seq > 0) {
            latest = seq;
            synchronized (newer) {
                newer.notifyAll();
            }
        }
    }

    /** Brings the schema up to {@link #SCHEMA_VERSION}, and returns the epoch. */
    private String migrate(Connection c) throws SQLException {
        long version = queryLong(c, "PRAGMA user_version");
        if (version > SCHEMA_VERSION) {
            throw new SQLException("events.db is at schema " + version
                    + ", written by a newer version of this plugin; this one knows up to " + SCHEMA_VERSION);
        }
        for (long next = version + 1; next <= SCHEMA_VERSION; next++) {
            try (Statement s = c.createStatement()) {
                s.executeUpdate(script(next) + "\nPRAGMA user_version = " + next + ";");
            }
            LOGGER.log(Level.INFO, "events.db migrated to schema {0}", next);
        }
        latest = queryLong(c, "SELECT last FROM sequence");
        return queryString(c, "SELECT value FROM meta WHERE key = 'epoch'");
    }

    private static String script(long version) throws SQLException {
        String name = "schema/" + version + ".sql";
        try (InputStream in = Store.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new SQLException("missing " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SQLException("cannot read " + name, e);
        }
    }

    private synchronized void close() throws SQLException {
        writer.close();
    }

    private static long nextSeq(Connection c) throws SQLException {
        return queryLong(c, "UPDATE sequence SET last = last + 1 RETURNING last");
    }

    private static String jobId(Connection c, String fullName) throws SQLException {
        return mintedId(c, "jobs", "full_name", fullName);
    }

    private static String nodeId(Connection c, String name) throws SQLException {
        return mintedId(c, "nodes", "name", name);
    }

    /** The live row's id for {@code name}, minting one on first sight. */
    private static String mintedId(Connection c, String table, String column, String name) throws SQLException {
        return queryString(c, "INSERT INTO " + table + " (id, " + column + ") VALUES (?, ?)"
                + " ON CONFLICT (" + column + ") WHERE deleted_at IS NULL"
                + " DO UPDATE SET " + column + " = excluded." + column + " RETURNING id",
                UUID.randomUUID().toString(), name);
    }

    private static long buildId(Connection c, String jobId, int number) throws SQLException {
        // Created by whichever arrives first: a freestyle build's executor is
        // taken before the build is reported as started.
        return queryLong(c, "INSERT INTO builds (job_id, number) VALUES (?, ?)"
                + " ON CONFLICT (job_id, number) DO UPDATE SET number = excluded.number RETURNING id", jobId, number);
    }

    private static int execute(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement s = prepare(c, sql, args)) {
            return s.executeUpdate();
        }
    }

    private static String queryString(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement s = prepare(c, sql, args); ResultSet r = s.executeQuery()) {
            return r.next() ? r.getString(1) : null;
        }
    }

    private static Long queryLong(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement s = prepare(c, sql, args); ResultSet r = s.executeQuery()) {
            if (!r.next()) {
                return null;
            }
            long value = r.getLong(1);
            return r.wasNull() ? null : value;
        }
    }

    private static PreparedStatement prepare(Connection c, String sql, Object... args) throws SQLException {
        PreparedStatement s = c.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            s.setObject(i + 1, args[i]);
        }
        return s;
    }
}
