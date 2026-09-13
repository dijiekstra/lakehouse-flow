package io.github.lakehouseflow.dao;

/**
 * Grouped task scheduling-state count used to refresh bounded backlog gauges.
 */
public interface TaskInstanceStateCount {

    /**
     * Return the scheduler-owned task state.
     *
     * @return task scheduling state
     */
    String getState();

    /**
     * Return the number of task instances in this state.
     *
     * @return grouped task instance count
     */
    long getInstanceCount();
}
