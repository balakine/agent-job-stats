package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.PeriodicWork;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Deletes builds, and their allocations, older than a configured age.
 *
 * <p>Off unless {@code -Dcom.varjo.jenkins.agentevents.Store.retentionDays=N}
 * is set: at a few hundred bytes a row, most instances can keep everything.
 * Read on every run, so it can be changed without a restart.
 */
@Extension
public class Retention extends PeriodicWork {

    private static final Logger LOGGER = Logger.getLogger(Retention.class.getName());

    static final String PROPERTY = Store.class.getName() + ".retentionDays";

    @Override
    public long getRecurrencePeriod() {
        return TimeUnit.DAYS.toMillis(1);
    }

    @Override
    protected void doRun() {
        int days = Integer.getInteger(PROPERTY, 0);
        if (days <= 0) {
            return;
        }
        long cutoff = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days);
        try {
            int deleted = Store.get().prune(cutoff);
            LOGGER.log(Level.FINE, "pruned {0} builds older than {1} days", new Object[] {deleted, days});
        } catch (SQLException | RuntimeException e) {
            LOGGER.log(Level.WARNING, "could not prune old builds", e);
        }
    }
}
