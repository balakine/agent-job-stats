package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.Computer;
import hudson.model.Item;
import hudson.model.RootAction;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import net.sf.json.JSONObject;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import jenkins.model.Jenkins;
import org.kohsuke.stapler.StaplerRequest2;
import org.kohsuke.stapler.StaplerResponse2;

/**
 * Streams the event log as newline-delimited JSON, resumable by sequence number:
 *
 * <pre>
 *   GET /agent-events/stream            everything still retained, then live
 *   GET /agent-events/stream?since=417  everything after 417, then live
 * </pre>
 *
 * <p>One JSON object per line, each an event's properties plus its {@code seq}.
 * Blank lines are keepalives. A {@code {"type":"gap"}} line says the requested
 * sequence has already fallen out of the backlog, so the reader knows its record
 * is incomplete instead of quietly missing events.
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
        Jenkins.get().checkPermission(Jenkins.READ);

        long since = parseSince(req.getParameter("since"));
        rsp.setContentType("application/x-ndjson;charset=UTF-8");
        rsp.setHeader("Cache-Control", "no-cache, no-store");
        // nginx buffers proxied responses by default, which would hold events
        // back until the connection closed. This header turns that off for this
        // response alone, so the stream needs no proxy configuration of its own.
        rsp.setHeader("X-Accel-Buffering", "no");

        EventLog log = EventLog.get();

        // Asking whether the endpoint exists is not a connection: no reader, no
        // held stream, nothing in the log.
        if (req.getParameter("probe") != null) {
            try (OutputStream out = rsp.getOutputStream()) {
                write(out, hello(log));
            }
            return;
        }

        long oldest = log.oldestSeq();
        long latest = log.latestSeq();
        // A cursor past the end belongs to a numbering that no longer exists,
        // which happens when Jenkins restarts. Replay everything still held
        // rather than the nothing an out-of-range cursor would select.
        boolean restarted = since > latest;
        if (restarted) {
            since = 0;
        }

        EventLog.Reader reader = new EventLog.Reader();
        List<EventLog.Entry> backlog = log.attach(reader, since);

        // At FINE because a client reconnects on every network blip and the
        // system log is read at INFO; enable this class in Manage Jenkins >
        // System Log to follow clients.
        String user = Jenkins.getAuthentication2().getName();
        long openedAt = System.currentTimeMillis();
        long delivered = 0;
        LOGGER.log(Level.FINE, "stream opened for {0} (since={1}, replaying {2})",
                new Object[] {user, since, backlog.size()});

        try (OutputStream out = rsp.getOutputStream()) {
            write(out, hello(log));
            if (restarted) {
                JSONObject gap = new JSONObject();
                gap.put("type", "gap");
                gap.put("reason", "log restarted");
                gap.put("oldest_available", oldest);
                write(out, line(gap));
            } else if (since > 0 && oldest > since + 1) {
                JSONObject gap = new JSONObject();
                gap.put("type", "gap");
                gap.put("requested_since", since);
                gap.put("oldest_available", oldest);
                write(out, line(gap));
            }
            for (EventLog.Entry entry : backlog) {
                writeEntry(out, entry);
                delivered++;
            }

            while (true) {
                EventLog.Entry entry = reader.poll(KEEPALIVE_SECONDS, TimeUnit.SECONDS);
                if (reader.hasLagged()) {
                    // This reader has lost events, which is worth seeing without
                    // anyone having turned logging up first.
                    LOGGER.log(Level.WARNING,
                            "disconnecting {0}: reader fell behind and lost events",
                            user);
                    JSONObject gap = new JSONObject();
                    gap.put("type", "gap");
                    gap.put("reason", "reader too slow");
                    write(out, line(gap));
                    return;
                }
                if (entry == null) {
                    // A blank line is the keepalive: one byte, proves the
                    // connection both ways, and a reader skips it for free.
                    write(out, "\n");
                } else {
                    writeEntry(out, entry);
                    delivered++;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            // The reader hung up. Nothing to report: that is how streams end.
        } finally {
            log.detach(reader);
            LOGGER.log(Level.FINE, "stream closed for {0} after {1}s, {2} events sent",
                    new Object[] {
                        user, (System.currentTimeMillis() - openedAt) / 1000, delivered
                    });
        }
    }

    /** The line every stream opens with: which numbering, and what it holds. */
    private static String hello(EventLog log) {
        JSONObject json = new JSONObject();
        json.put("type", "hello");
        json.put("epoch", log.epoch());
        json.put("oldest", log.oldestSeq());
        json.put("latest", log.latestSeq());
        return line(json);
    }

    private static String line(JSONObject json) {
        return json.toString() + "\n";
    }

    /** Zero - the default - selects everything still held. */
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

    private void writeEntry(OutputStream out, EventLog.Entry entry) throws IOException {
        String line = render(entry);
        if (line != null) {
            write(out, line);
        }
    }

    /**
     * Renders one event for the user making this request, or null when they may
     * not see it at all.
     */
    private String render(EventLog.Entry entry) {
        if (!entry.event.isReadable()) {
            return null;
        }

        // A copy, because the redaction below must not touch the stored event.
        Map<String, Object> props = new LinkedHashMap<>(entry.event.properties());

        // Which agent ran the work is a fact about the agent, not only about the
        // job, so it takes a permission on the agent.
        Object node = props.get("node");
        if (node != null && !canSeeAgent(node.toString())) {
            props.remove("node");
            props.remove("executor");
            props.put("node_hidden", "true");
        }

        // seq first, then the event's own properties in the order it set them.
        JSONObject json = new JSONObject();
        json.put("seq", entry.seq);
        props.forEach(json::put);
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
     * checked, so it is hidden: failing closed is the only safe way to be wrong
     * about a permission.
     */
    private boolean canSeeAgent(String node) {
        Jenkins jenkins = Jenkins.get();
        Computer computer = ExecutorEventPublisher.BUILT_IN.equals(node)
                ? jenkins.toComputer()
                : jenkins.getComputer(node);
        return computer != null && computer.hasPermission(Computer.EXTENDED_READ);
    }
}
