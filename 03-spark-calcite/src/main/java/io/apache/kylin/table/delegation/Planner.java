package io.apache.kylin.table.delegation;

/**
 * The planner interface is the bridge between base API and different planner modules.
 */
public interface Planner {

    /**
     * Retrieves a {@link Parser} that provides methods for parsing a SQL string.
     *
     * @return initialized {@link Parser}
     */
    Parser getParser();
}
