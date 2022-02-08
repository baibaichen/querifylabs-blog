package org.apache.kylin.table.delegation;

import org.apache.kylin.core.Transformation;
import org.apache.kylin.table.operations.ModifyOperation;

import java.util.List;

/**
 * This interface serves two purposes:
 *
 * <ul>
 *   <li>SQL parser via {@link #getParser()} - transforms a SQL string into a Table API specific
 *       objects e.g. tree of {@link Operation}s
 *   <li>relational planner - provides a way to plan, optimize and transform tree of {@link
 *       ModifyOperation} into a runnable form ({@link Transformation})
 * </ul>
 *
 * <p>The Planner is execution agnostic. It is up to the {@link
 * org.apache.flink.table.api.TableEnvironment} to ensure that if any of the {@link QueryOperation}
 * pull any runtime configuration, all those configurations are equivalent. Example: If some of the
 * {@link QueryOperation}s scan DataStreams, all those DataStreams must come from the same
 * StreamExecutionEnvironment, because the result of {@link Planner#translate(List)} will strip any
 * execution configuration from the DataStream information.
 *
 * <p>All Tables referenced in either {@link Parser#parse(String)} or {@link
 * Planner#translate(List)} should be previously registered in a {@link
 * org.apache.flink.table.catalog.CatalogManager}, which will be provided during instantiation of
 * the {@link Planner}.
 */
public interface Planner {

    /**
     * Retrieves a {@link Parser} that provides methods for parsing a SQL string.
     *
     * @return initialized {@link Parser}
     */
    Parser getParser();

    /**
     * Converts a relational tree of {@link ModifyOperation}s into a set of runnable {@link
     * Transformation}s.
     *
     * <p>This method accepts a list of {@link ModifyOperation}s to allow reusing common subtrees of
     * multiple relational queries. Each query's top node should be a {@link ModifyOperation} in
     * order to pass the expected properties of the output {@link Transformation} such as output
     * mode (append, retract, upsert) or the expected output type.
     *
     * @param modifyOperations list of relational operations to plan, optimize and convert in a
     *     single run.
     * @return list of corresponding {@link Transformation}s.
     */
    List<Transformation<?>> translate(List<ModifyOperation> modifyOperations);
}
