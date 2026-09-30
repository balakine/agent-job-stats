package com.varjo.jenkins.agentevents;

import hudson.Extension;
import hudson.model.Item;
import hudson.model.Node;
import hudson.model.listeners.ItemListener;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import jenkins.model.NodeListener;

/**
 * Updates the stored name behind each job and node id on rename, move and
 * deletion. A rename made while the plugin is not running is missed: the job's
 * next build gets a new id under the new name.
 */
final class IdentityEvents {

    private static final Logger LOGGER = Logger.getLogger(IdentityEvents.class.getName());

    private IdentityEvents() {}

    /** Renames and moves of jobs and folders, and their deletion. */
    @Extension
    public static class Items extends ItemListener {

        /** Covers both a rename and a move, which Jenkins reports the same way. */
        @Override
        public void onLocationChanged(Item item, String oldFullName, String newFullName) {
            try {
                Store.get().itemMoved(oldFullName, newFullName, System.currentTimeMillis());
            } catch (SQLException | RuntimeException e) {
                LOGGER.log(Level.WARNING, "could not follow " + oldFullName + " to " + newFullName, e);
            }
        }

        @Override
        public void onDeleted(Item item) {
            try {
                Store.get().itemDeleted(item.getFullName(), System.currentTimeMillis());
            } catch (SQLException | RuntimeException e) {
                LOGGER.log(Level.WARNING, "could not record the deletion of " + item.getFullName(), e);
            }
        }
    }

    /** Renames and deletion of agents. */
    @Extension
    public static class Nodes extends NodeListener {

        @Override
        protected void onUpdated(Node oldOne, Node newOne) {
            String from = oldOne.getNodeName();
            String to = newOne.getNodeName();
            if (from.equals(to)) {
                return;
            }
            try {
                Store.get().nodeRenamed(from, to, System.currentTimeMillis());
            } catch (SQLException | RuntimeException e) {
                LOGGER.log(Level.WARNING, "could not follow agent " + from + " to " + to, e);
            }
        }

        @Override
        protected void onDeleted(Node node) {
            try {
                Store.get().nodeDeleted(node.getNodeName(), System.currentTimeMillis());
            } catch (SQLException | RuntimeException e) {
                LOGGER.log(Level.WARNING, "could not record the deletion of agent " + node.getNodeName(), e);
            }
        }
    }
}
