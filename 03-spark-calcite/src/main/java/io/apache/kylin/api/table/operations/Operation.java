package io.apache.kylin.api.table.operations;

/**
 * Covers all sort of Table operations such as queries(DQL), modifications(DML), definitions(DDL),
 * or control actions(DCL). This is the output of {@link io.apache.kylin.api.table.delegation.Planner#getParser()} and {@link
 * io.apache.kylin.api.table.delegation.Parser#parse(String)}.
 *
 * @see QueryOperation
 * @see ModifyOperation
 */

public interface Operation {

    /**
     * Returns a string that summarizes this operation for printing to a console. An implementation
     * might skip very specific properties.
     *
     * @return summary string of this operation for debugging purposes
     */
    String asSummaryString();
}
