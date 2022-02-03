package io.apache.kylin.api.table.operations;

import java.util.List;

/**
 * Base class for representing an operation structure behind a user-facing {@link Table} API.
 *
 * <p>It represents an operation that can be a node of a relational query. It has a schema, that can
 * be used to validate a {@link QueryOperation} applied on top of this one.
 */
public interface QueryOperation extends Operation {

    //TODO: schema

    /**
     * Returns a string that fully serializes this instance. The serialized string can be used for
     * storing the query in e.g. a {@link org.apache.flink.table.catalog.Catalog} as a view.
     *
     * @return detailed string for persisting in a catalog
     * @see Operation#asSummaryString()
     */
    default String asSerializableString() {
        throw new UnsupportedOperationException(
                "QueryOperations are not string serializable for now.");
    }

    List<QueryOperation> getChildren();

    default <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
