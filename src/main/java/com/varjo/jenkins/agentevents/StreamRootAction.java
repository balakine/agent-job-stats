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
 * are keepalives. A {@code {"type":"gap"}} line says some requested events are
 * not stored: they were pruned, or the cursor is from a recreated database.
 *
 * <p>Each stream reads the database from its own cursor, so a slow reader
 * catches up from where it is and nothing is buffered for it. Events are
 * filtered by the reader's own permissions.
 */
@Extension
public class StreamRootAction implements RootAction {

    private static final Logger LOGGER = Logger.getLogger(StreamRootAction.class.getName());

    /** Short enough to keep proxies from timing out an idle stream, and to detect a dead peer. */
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
        // Turns off nginx's proxy buffering for this response, which would
        // otherwise hold events back until the connection closed.
        rsp.setHeader("X-Accel-Buffering", "no");

        Store store = Store.get();
        String user = Jenkins.getAuthentication2().getName();
        // Permissions on deleted jobs and agents cannot be checked; their events
        // go to administrators only.
        boolean admin = jenkins.hasPermission(Jenkins.ADMINISTER);
        long openedAt = System.currentTimeMillis();
        long delivered = 0;
        boolean probe = req.getParameter("probe") != null;

        try (Connection db = store.openReader(); OutputStream out = rsp.getOutputStream()) {
            long oldest = store.oldestSeq(db);
            long latest = store.latestSeq();
            write(out, hello(store.epoch(), oldest, latest));
            // A probe gets the hello line only, and is not logged.
            if (probe) {
                return;
            }

            // A cursor past the end is from a recreated database.
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

            // FINE: clients reconnect after every network interruption.
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
                    // Keepalive: a blank line, which readers skip. Writing it
                    // is also what detects a dead peer.
                    write(out, "\n");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "stream for " + user + " failed reading the event store", e);
        } catch (IOException e) {
            // The client disconnected: the normal end of a stream.
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
            // Agent details need permission on the agent as well as the job.
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
     * Whether the caller has {@code Computer.EXTENDED_READ} on this agent, the
     * narrowest permission Jenkins defines for reading an agent's details. A
     * deleted agent cannot be checked and is visible to administrators only.
     */
    private static boolean canSeeAgent(String node, boolean deleted, boolean admin) {
        if (deleted) {
            return admin;
        }
        Jenkins jenkins = Jenkins.get();
        // Nodes are recorded by their self label, which for Jenkins itself is not its computer name.
        Computer computer = node.equals(jenkins.getSelfLabel().getName())
                ? jenkins.toComputer()
                : jenkins.getComputer(node);
        return computer != null && computer.hasPermission(Computer.EXTENDED_READ);
    }
}
