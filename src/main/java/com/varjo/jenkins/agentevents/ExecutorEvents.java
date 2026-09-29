package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.Executor;
import hudson.model.ExecutorListener;
import hudson.model.Node;
import hudson.model.OneOffExecutor;
import hudson.model.Queue;
import hudson.model.Run;
import hudson.slaves.AbstractCloudSlave;
import hudson.slaves.EphemeralNode;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Records an executor taking up work and handing it back.
 *
 * <p>Everything recorded here comes from {@link ExecutorListener} itself, which
 * knows nothing about what kind of job it is serving. That keeps one meaning for
 * every allocation whatever ran: a Pipeline {@code node} block, a freestyle
 * build, a matrix configuration. Whether the build passed is a separate
 * question, answered by the build's own result.
 *
 * <p>Only static agents are tracked. A cloud agent exists for one build under a
 * generated name, so giving it a stable identity would mean nothing.
 */
@Extension
public class ExecutorEvents implements ExecutorListener {

    private static final Logger LOGGER = Logger.getLogger(ExecutorEvents.class.getName());

    /** Node name recorded for Jenkins's own built-in node, which has none. */
    static final String BUILT_IN = "built-in";

    /** The two outcomes {@link ExecutorListener} distinguishes, and no more. */
    static final String COMPLETED = "completed";

    static final String PROBLEMS = "problems";

    /** Keeps a stray exception message from bloating the row. */
    private static final int PROBLEM_LIMIT = 300;

    /**
     * Recorded from {@code taskStarted} rather than {@code taskAccepted}: the
     * two are microseconds apart, but only once the task has started is the
     * executable - and through it the build the executor works for - knowable.
     */
    @Override
    public void taskStarted(Executor executor, Queue.Task task) {
        Run<?, ?> run = trackedRun(executor);
        if (run == null) {
            return;
        }
        try {
            Store.get().executorAllocated(run.getParent().getFullName(), run.getNumber(), nodeName(executor),
                    executor.getNumber(), task.getDisplayName(), System.currentTimeMillis());
        } catch (SQLException | RuntimeException e) {
            // A monitoring plugin must never fail the build it is watching.
            LOGGER.log(Level.WARNING, "could not record an allocation on " + nodeName(executor), e);
        }
    }

    @Override
    public void taskCompleted(Executor executor, Queue.Task task, long durationMs) {
        released(executor, durationMs, COMPLETED, null);
    }

    @Override
    public void taskCompletedWithProblems(
            Executor executor, Queue.Task task, long durationMs, Throwable problems) {
        released(executor, durationMs, PROBLEMS, problems);
    }

    private void released(Executor executor, long durationMs, String outcome, Throwable problems) {
        Run<?, ?> run = trackedRun(executor);
        if (run == null) {
            return;
        }
        try {
            Store.get().executorReleased(run.getParent().getFullName(), run.getNumber(), nodeName(executor),
                    executor.getNumber(), System.currentTimeMillis(), outcome, durationMs,
                    problems == null ? null : summarize(problems));
        } catch (SQLException | RuntimeException e) {
            LOGGER.log(Level.WARNING, "could not record a release on " + nodeName(executor), e);
        }
    }

    /**
     * The build this executor is working for, or null when the work is not
     * tracked: a flyweight executor, an ephemeral agent, or work that belongs to
     * no build.
     *
     * <p>A flyweight executor is where a Pipeline's own task sits while it waits
     * on its {@code node} blocks; it occupies no agent capacity and lives as
     * long as the build, which {@link BuildEvents} already records.
     */
    private static Run<?, ?> trackedRun(Executor executor) {
        if (executor instanceof OneOffExecutor) {
            return null;
        }
        Node node = executor.getOwner().getNode();
        if (node == null || node instanceof EphemeralNode || node instanceof AbstractCloudSlave) {
            return null;
        }
        return runOf(executor.getCurrentExecutable());
    }

    /**
     * Walks up from an executable to the build it belongs to. Some executables
     * are nested inside another - a Pipeline {@code node} block's placeholder
     * belongs to the build running elsewhere - so the build can be a step or two
     * above. The bound only guards against a cycle.
     */
    private static Run<?, ?> runOf(Queue.Executable executable) {
        Queue.Executable current = executable;
        for (int guard = 0; current != null && guard < 10; guard++) {
            if (current instanceof Run) {
                return (Run<?, ?>) current;
            }
            Queue.Executable parent = current.getParentExecutable();
            if (parent == current) {
                break;
            }
            current = parent;
        }
        return null;
    }

    static String nodeName(Executor executor) {
        String name = executor.getOwner().getName();
        return name == null || name.isEmpty() ? BUILT_IN : name;
    }

    private static String summarize(Throwable problems) {
        String text = problems.getMessage();
        if (text == null || text.isEmpty()) {
            text = problems.getClass().getName();
        }
        return text.length() <= PROBLEM_LIMIT ? text : text.substring(0, PROBLEM_LIMIT) + "...";
    }
}
