package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.model.listeners.RunListener;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Records a build starting and ending, for every kind of job.
 *
 * <p>Together with {@link ExecutorEvents} that is the whole event set: a build
 * ran, it occupied these agents, it ended like this. A queue item that never
 * becomes a build is not a build, so the queue is not watched.
 */
@Extension
public class BuildEvents extends RunListener<Run<?, ?>> {

    private static final Logger LOGGER = Logger.getLogger(BuildEvents.class.getName());

    @Override
    public void onStarted(Run<?, ?> run, TaskListener listener) {
        try {
            Store.get().buildStarted(run.getParent().getFullName(), run.getNumber(), System.currentTimeMillis());
        } catch (SQLException | RuntimeException e) {
            // A monitoring plugin must never fail the build it is watching.
            LOGGER.log(Level.WARNING, "could not record the start of " + run, e);
        }
    }

    @Override
    public void onCompleted(Run<?, ?> run, TaskListener listener) {
        // By onCompleted the result is set, which onFinalized would also give
        // us but later, after other listeners have had their turn.
        Result result = run.getResult();
        try {
            Store.get().buildEnded(run.getParent().getFullName(), run.getNumber(), System.currentTimeMillis(),
                    result == null ? null : result.toString());
        } catch (SQLException | RuntimeException e) {
            LOGGER.log(Level.WARNING, "could not record the end of " + run, e);
        }
    }
}
