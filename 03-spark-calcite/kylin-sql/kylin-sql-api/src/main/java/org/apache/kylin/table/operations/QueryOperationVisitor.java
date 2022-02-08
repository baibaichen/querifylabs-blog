package org.apache.kylin.table.operations;

/**
 * Class that implements visitor pattern. It allows type safe logic on top of tree of {@link
 * QueryOperation}s.
 */

public interface QueryOperationVisitor<T> {
    T visit(QueryOperation other);
}
