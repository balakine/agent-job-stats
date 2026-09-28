package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.Executor;
import hudson.model.ExecutorListener;
import hudson.model.OneOffExecutor;
import hudson.model.Queue;
import hudson.model.Run;

/**
 * Records an event whenever an executor takes up work or hands it back.
 *
 * <p>{@link JobEvents} reports the build itself; Jenkins has no notion of an
 * event for an executor being taken or given back. These two fill that gap:
 *
 * <ul>
 *   <li>{@code executor_allocated} - an executor on a node took up a task</li>
 *   <li>{@code executor_released} - the task left that executor</li>
 * </ul>
 *
 * <p>Everything recorded here comes from {@link ExecutorListener} itself, which
 * knows nothing about what kind of job it is serving. That keeps one meaning for
 * every event whatever ran: a Pipeline {@code node} block, a freestyle build, a
 * matrix configuration or anything else that occupies an executor. Whether the
 * build passed is a separate question, answered by {@code status} on
 * {@code build_ended}.
 *
 * <p>They go into the same log as the run events, so that one stream tells the
 * whole story in order, and each carries the job whose {@code Item.READ} decides
 * who may see it.
 */
@Extension
public class ExecutorEventPublisher implements ExecutorListener {

    static final String ALLOCATED = "executor_allocated";
    static final String RELEASED = "executor_released";

    /** Node name reported for Jenkins's own built-in node, which has none. */
    static final String BUILT_IN = "built-in";

    /** The two outcomes {@link ExecutorListener} distinguishes, and no more. */
    static final String COMPLETED = "completed";

    static final String PROBLEMS = "problems";

    /** Keeps a stray exception message from bloating the event. */
    private static final int PROBLEM_LIMIT = 300;

    /**
     * Published from {@code taskStarted} rather than {@code taskAccepted}: the
     * two are microseconds apart, but only once the task has started is the
     * executable - and through it the run the executor is working for -
     * knowable.
     */
    @Override
    public void taskStarted(Executor executor, Queue.Task task) {
        record(ALLOCATED, executor, task, null, null, null);
    }

    @Override
    public void taskCompleted(Executor executor, Queue.Task task, long durationMs) {
        record(RELEASED, executor, task, durationMs, COMPLETED, null);
    }

    @Override
    public void taskCompletedWithProblems(
            Executor executor, Queue.Task task, long durationMs, Throwable problems) {
        record(RELEASED, executor, task, durationMs, PROBLEMS, problems);
    }

    private void record(
            String name,
            Executor executor,
            Queue.Task task,
            Long durationMs,
            String outcome,
            Throwable problems) {
        if (isFlyweight(executor)) {
            return;
        }
        Run<?, ?> run = runOf(executor.getCurrentExecutable());
        if (run == null) {
            // Work that is not part of a build. Nothing to attribute it to, and
            // no job whose permissions could decide who sees it.
            return;
        }
        Event event = Event.forRun(name, run);

        // Node plus executor number identifies one slot, which is what pairs a
        // release with its allocation when a build takes two executors on one
        // agent.
        event.set("node", nodeName(executor))
                .set("executor", executor.getNumber())
                .set("task", task.getDisplayName());
        if (outcome != null) {
            event.set("outcome", outcome).set("duration_ms", durationMs);
        }
        if (problems != null) {
            event.set("problem", summarize(problems));
        }
        EventLog.get().record(event);
    }

    /**
     * A flyweight executor is where a Pipeline's own task sits while it waits on
     * its {@code node} blocks; it occupies no agent capacity and its lifetime is
     * the build's lifetime, which {@code build_started} and {@code build_ended}
     * already report. Reporting it again would just double every build.
     */
    private static boolean isFlyweight(Executor executor) {
        return executor instanceof OneOffExecutor;
    }

    /**
     * Walks up from an executable to the run it belongs to. Some executables are
     * nested inside another - work handed to an agent on behalf of a build that
     * is itself executing elsewhere - so the run can be a step or two above.
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

    private static String nodeName(Executor executor) {
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
