package com.varjo.jenkins.agentevents;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.model.Item;
import hudson.model.Run;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One event: a flat map of properties plus the job it belongs to.
 *
 * <p>The vocabulary is small on purpose. An event's name says what happened, so
 * no property repeats it, and a property appears only when it carries something
 * the name does not: {@code status} on a finished build, {@code node} on an
 * executor event. Nothing is included merely because Jenkins could supply it.
 *
 * <p>Values are strings or numbers and are serialized as such, so a property's
 * JSON type follows from what it is rather than from a list of exceptions.
 *
 * <p>The job is held as a reference, not a name, so that who may read the event
 * is decided when it is read rather than when it is recorded - and so that a
 * build's events stay readable even if the job is deleted afterwards, which a
 * lookup by name could not manage.
 */
final class Event {

    private final Map<String, Object> props = new LinkedHashMap<>();

    private final Item job;

    /** An event about one build. Every event is. */
    static Event forRun(String name, @NonNull Run<?, ?> run) {
        return new Event(name, run.getParent())
                .set("url", run.getUrl())
                .set("build", run.getNumber());
    }

    private Event(String name, Item job) {
        this.job = job;
        set("event", name);
        set("at", System.currentTimeMillis());
        set("job", job.getFullName());
    }

    Event set(String key, @CheckForNull String value) {
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
        return this;
    }

    Event set(String key, long value) {
        props.put(key, value);
        return this;
    }

    Map<String, Object> properties() {
        return Collections.unmodifiableMap(props);
    }

    /** Whether the caller may see this event, by {@code Item.READ} on its job. */
    boolean isReadable() {
        return job.hasPermission(Item.READ);
    }
}
