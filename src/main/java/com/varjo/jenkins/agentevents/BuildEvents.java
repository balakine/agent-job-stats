package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.model.listeners.RunListener;

/**
 * Records a build starting and ending, for every kind of job.
 *
 * <p>Together with {@link ExecutorEventPublisher} that is the whole event set:
 * a build ran, it occupied these agents, it ended like this. Jenkins offers
 * listeners for queue movement and for job configuration too, and neither is
 * used here - a queue item that never becomes a build is not a build, and a
 * configuration change is not activity.
 */
@Extension
public class BuildEvents extends RunListener<Run<?, ?>> {

    @Override
    public void onStarted(Run<?, ?> run, TaskListener listener) {
        EventLog.get().record(Event.forRun("build_started", run));
    }

    @Override
    public void onCompleted(Run<?, ?> run, TaskListener listener) {
        // By onCompleted the result is set, which onFinalized would also give
        // us but later, after other publishers have had their turn.
        Result result = run.getResult();
        EventLog.get().record(Event.forRun("build_ended", run)
                .set("status", result == null ? null : result.toString()));
    }
}
