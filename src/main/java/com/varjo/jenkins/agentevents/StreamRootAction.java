package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.Computer;
import hudson.model.Item;
import hudson.model.RootAction;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import jenkins.model.Jenkins;
import net.sf.json.JSONObject;
import org.kohsuke.stapler.StaplerRequest2;
import org.kohsuke.stapler.StaplerResponse2;

/**
 * Streams stored events as newline-delimited JSON, resumable by sequence number:
 *
 * <pre>
 *   GET /agent-events/stream            everything stored, then live
 *   GET /agent-events/stream?since=417  everything after 417, then live
 * </pre>
 *
 * <p>One JSON object per line, each an event with its {@code seq}. Blank lines
 * are keepalives. A {@code {"type":"gap"}} line says the requested sequence is
 * no longer stored, so the reader knows its record is incomplete instead of
 * quietly missing events.
 *
 * <p>Events are read back from the database, so a reader that falls behind is
 * simply behind: it catches up from where it is, and nothing is buffered for it.
 *
 * <p>NDJSON rather than a framed format because a reader needs only a line and
 * a JSON parse, and because the filtering below has to happen per reader: what
 * one may see depends on their permissions, so the same event renders
 * differently for different callers and cannot be broadcast.
 */
@Extension
public class StreamRootAction implements RootAction {

    private static final Logger LOGGER = Logger.getLogger(StreamRootAction.class.getName());

    /** Kept short so a proxy or a dead peer is noticed without waiting long. */
    private static final long KEEPALIVE_SECONDS = 15;

    /** Rows read per query while catching up. */
    private static final int BATCH = 500;

    @Override
    public String getUrlName() {
        return "agent-events";
    }

    /** Not a page; nothing to show in the UI. */
    @Override
    public String getIconFileName() {
        return null;
    }

    @Override
    public String getDisplayName() {
        return null;
    }

    public void doStream(StaplerRequest2 req, StaplerResponse2 rsp) throws IOException {
        Jenkins jenkins = Jenkins.get();
        jenkins.checkPermission(Jenkins.READ);

        long since = parseSince(req.getParameter("since"));
        rsp.setContentType("application/x-ndjson;charset=UTF-8");
        rsp.setHeader("Cache-Control", "no-cache, no-store");
        // nginx buffers proxied responses by default, which would hold events
        // back until the connection closed. This header turns that off for this
        // response alone, so the stream needs no proxy configuration of its own.
        rsp.setHeader("X-Accel-Buffering", "no");

        Store store = Store.get();
        String user = Jenkins.getAuthentication2().getName();
        // Deleted jobs and agents can no longer be asked who may see them, so
        // their history is shown only to administrators.
        boolean admin = jenkins.hasPermission(Jenkins.ADMINISTER);
        long openedAt = System.currentTimeMillis();
        long delivered = 0;
        boolean probe = req.getParameter("probe") != null;

        try (Connection db = store.openReader(); OutputStream out = rsp.getOutputStream()) {
            long oldest = store.oldestSeq(db);
            long latest = store.latestSeq();
            write(out, hello(store.epoch(), oldest, latest));
            // Asking whether the endpoint exists is not a connection: nothing
            // follows the greeting, and nothing is logged.
            if (probe) {
                return;
            }

            // A cursor past the end belongs to a numbering that no longer
            // exists, which happens when the database is recreated.
            boolean reset = since > latest;
            if (reset || (since > 0 && oldest > since + 1)) {
                JSONObject gap = new JSONObject();
                gap.put("type", "gap");
                gap.put("requested_since", since);
                gap.put("oldest_available", oldest);
                write(out, line(gap));
            }
            if (reset) {
                since = 0;
            }

            // At FINE because a client reconnects on every network blip and the
            // system log is read at INFO; enable this class in Manage Jenkins >
            // System Log to follow clients.
            LOGGER.log(Level.FINE, "stream opened for {0} (since={1})", new Object[] {user, since});

            long cursor = since;
            while (true) {
                List<Store.Row> rows = store.eventsAfter(db, cursor, BATCH);
                for (Store.Row row : rows) {
                    String line = render(row, admin);
                    if (line != null) {
                        write(out, line);
                        delivered++;
                    }
                    cursor = row.seq();
                }
                if (rows.size() == BATCH) {
                    continue;
                }
                if (!store.awaitNewer(cursor, KEEPALIVE_SECONDS, TimeUnit.SECONDS)) {
                    // A blank line is the keepalive: one byte, proves the
                    // connection both ways, and a reader skips it for free.
                    write(out, "\n");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "stream for " + user + " failed reading the event store", e);
        } catch (IOException e) {
            // The reader hung up. Nothing to report: that is how streams end.
        } finally {
            if (!probe) {
                LOGGER.log(Level.FINE, "stream closed for {0} after {1}s, {2} events sent",
                        new Object[] {user, (System.currentTimeMillis() - openedAt) / 1000, delivered});
            }
        }
    }

    /** The line every stream opens with: which numbering, and what it holds. */
    private static String hello(String epoch, long oldest, long latest) {
        JSONObject json = new JSONObject();
        json.put("type", "hello");
        json.put("epoch", epoch);
        json.put("oldest", oldest);
        json.put("latest", latest);
        return line(json);
    }

    private static String line(JSONObject json) {
        return json.toString() + "\n";
    }

    /** Zero - the default - selects everything stored. */
    private static long parseSince(String value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }
        try {
            return Math.max(Long.parseLong(value.trim()), 0);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static void write(OutputStream out, String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /**
     * Renders one event for the user making this request, or null when they may
     * not see it. Names are the job's and agent's current ones, so history
     * follows a rename.
     */
    private static String render(Store.Row row, boolean admin) {
        Item job = row.jobDeleted() ? null : Jenkins.get().getItemByFullName(row.jobName(), Item.class);
        boolean readable = row.jobDeleted() ? admin : job != null && job.hasPermission(Item.READ);
        if (!readable) {
            return null;
        }

        JSONObject json = new JSONObject();
        json.put("seq", row.seq());
        json.put("event", row.event());
        json.put("at", row.at());
        json.put("job", row.jobName());
        json.put("job_id", row.jobId());
        if (job != null) {
            json.put("url", job.getUrl() + row.build() + "/");
        }
        json.put("build", row.build());
        if (row.result() != null) {
            json.put("status", row.result());
        }
        if (row.nodeName() != null) {
            // Which agent ran the work is a fact about the agent, not only about
            // the job, so it takes a permission on the agent.
            if (canSeeAgent(row.nodeName(), row.nodeDeleted(), admin)) {
                json.put("node", row.nodeName());
                json.put("node_id", row.nodeId());
                json.put("executor", row.executor());
            } else {
                json.put("node_hidden", true);
            }
        }
        if (row.task() != null) {
            json.put("task", row.task());
        }
        if (row.outcome() != null) {
            json.put("outcome", row.outcome());
            json.put("duration_ms", row.durationMs());
        }
        if (row.problem() != null) {
            json.put("problem", row.problem());
        }
        return line(json);
    }

    /**
     * Whether the caller may know this agent by name, by
     * {@code Computer.EXTENDED_READ} on it.
     *
     * <p>Jenkins core has no per-agent read permission - {@code Computer}
     * defines CONFIGURE, EXTENDED_READ, DELETE, CREATE, DISCONNECT, CONNECT and
     * BUILD, and nothing finer - so EXTENDED_READ is the closest thing to "may
     * see this agent's details". An agent that no longer exists cannot be
     * checked, so only administrators see it: failing closed is the only safe
     * way to be wrong about a permission.
     */
    private static boolean canSeeAgent(String node, boolean deleted, boolean admin) {
        if (deleted) {
            return admin;
        }
        Jenkins jenkins = Jenkins.get();
        Computer computer = ExecutorEvents.BUILT_IN.equals(node)
                ? jenkins.toComputer()
                : jenkins.getComputer(node);
        return computer != null && computer.hasPermission(Computer.EXTENDED_READ);
    }
}
