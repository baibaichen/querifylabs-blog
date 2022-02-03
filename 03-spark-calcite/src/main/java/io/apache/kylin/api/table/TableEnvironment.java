package io.apache.kylin.api.table;


/**
 * A table environment is the base class, entry point, and central context for creating Table and
 * SQL API programs.
 *
 * <p>It is unified both on a language level for all JVM-based languages (i.e. there is no
 * distinction between Scala and Java API) and for bounded and unbounded data processing.
 *
 * <p>A table environment is responsible for:
 *
 * <ul>
 *   <li>Connecting to external systems.
 *   <li>Registering and retrieving {@link Table}s and other meta objects from a catalog.
 *   <li>Executing SQL statements.
 *   <li>Offering further configuration options.
 * </ul>
 *
 * <p>The syntax for path in methods such as {@link #createTemporaryView(String, Table)} is
 * following {@code [[catalog-name.]database-name.]object-name}, where the catalog name and database
 * are optional. For path resolution see {@link #useCatalog(String)} and {@link
 * #useDatabase(String)}.
 *
 * <p>Example: {@code `cat.1`.`db`.`Table`} resolves to an object named 'Table' in a catalog named
 * 'cat.1' and database named 'db'.
 *
 * <p>Note: This environment is meant for pure table programs. If you would like to convert from or
 * to other Flink APIs, it might be necessary to use one of the available language-specific table
 * environments in the corresponding bridging modules.
 */
public interface TableEnvironment {

    /**
     * Executes the given single statement and returns the execution result.
     *
     * <p>The statement can be DDL/DML/DQL/SHOW/DESCRIBE/EXPLAIN/USE. For DML and DQL, this method
     * returns {@link TableResult} once the job has been submitted. For DDL and DCL statements,
     * {@link TableResult} is returned once the operation has finished.
     *
     * <p>If multiple pipelines should insert data into one or more sink tables as part of a single
     * execution, use a {@link StatementSet} (see {@link TableEnvironment#createStatementSet()}).
     *
     * <p>By default, all DML operations are executed asynchronously. Use {@link
     * TableResult#await()} or {@link TableResult#getJobClient()} to monitor the execution. Set
     * {@link TableConfigOptions#TABLE_DML_SYNC} for always synchronous execution.
     *
     * @return content for DQL/SHOW/DESCRIBE/EXPLAIN, the affected row count for `DML` (-1 means
     *     unknown), or a string message ("OK") for other statements.
     */
    TableResult executeSql(String statement);
}
