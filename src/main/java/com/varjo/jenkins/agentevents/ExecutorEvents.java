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
 * Records an executor taking up work and handing it back, for any kind of task.
 * The outcome is the executor's, as {@link ExecutorListener} reports it, not the
 * build's result.
 */
@Extension
public class ExecutorEvents implements ExecutorListener {

    private static final Logger LOGGER = Logger.getLogger(ExecutorEvents.class.getName());

    private static final String COMPLETED = "completed";

    private static final String PROBLEMS = "problems";

    private static final int PROBLEM_LIMIT = 300;

    /**
     * The executable, and through it the build, is set by {@code taskStarted}.
     * The time recorded is the executor's start time, which Jenkins measures
     * the task's duration from.
     */
    @Override
    public void taskStarted(Executor executor, Queue.Task task) {
        Node node = executor.getOwner().getNode();
        Run<?, ?> run = trackedRun(executor, node);
        if (run == null) {
            return;
        }
        String name = node.getSelfLabel().getName();
        try {
            Store.get().executorAllocated(run.getParent().getFullName(), run.getNumber(), name,
                    executor.getNumber(), task.getDisplayName(), System.currentTimeMillis() - executor.getElapsedTime());
        } catch (SQLException | RuntimeException e) {
            // A recording failure must not fail the build.
            LOGGER.log(Level.WARNING, "could not record an allocation on " + name, e);
        }
    }

    @Override
    public void taskCompleted(Executor executor, Queue.Task task, long durationMs) {
        released(executor, COMPLETED, null);
    }

    @Override
    public void taskCompletedWithProblems(
            Executor executor, Queue.Task task, long durationMs, Throwable problems) {
        released(executor, PROBLEMS, problems);
    }

    private void released(Executor executor, String outcome, Throwable problems) {
        Node node = executor.getOwner().getNode();
        Run<?, ?> run = trackedRun(executor, node);
        if (run == null) {
            return;
        }
        String name = node.getSelfLabel().getName();
        try {
            Store.get().executorReleased(run.getParent().getFullName(), run.getNumber(), name,
                    executor.getNumber(), System.currentTimeMillis(), outcome,
                    problems == null ? null : summarize(problems));
        } catch (SQLException | RuntimeException e) {
            LOGGER.log(Level.WARNING, "could not record a release on " + name, e);
        }
    }

    /**
     * The build this executor is working for, or null when the work is not
     * tracked: a flyweight executor, an ephemeral agent, or work that belongs to
     * no build.
     *
     * <p>A flyweight executor holds a Pipeline's own task for the whole build
     * and occupies no agent capacity.
     */
    private static Run<?, ?> trackedRun(Executor executor, Node node) {
        if (executor instanceof OneOffExecutor
                || node == null
                || node instanceof EphemeralNode
                || node instanceof AbstractCloudSlave) {
            return null;
        }
        // A build's own executable, or a part of one such as a Pipeline node
        // block's placeholder, whose parent is the build.
        Queue.Executable executable = executor.getCurrentExecutable();
        if (executable instanceof Run<?, ?> run) {
            return run;
        }
        return executable != null && executable.getParentExecutable() instanceof Run<?, ?> run ? run : null;
    }

    private static String summarize(Throwable problems) {
        String text = problems.getMessage();
        if (text == null || text.isEmpty()) {
            text = problems.getClass().getName();
        }
        return text.length() <= PROBLEM_LIMIT ? text : text.substring(0, PROBLEM_LIMIT) + "...";
    }
}
