package org.apache.kylin.table.operations;

/**
 * Class that implements visitor pattern. It allows type safe logic on top of tree of {@link
 * ModifyOperation}s.
 */
public interface ModifyOperationVisitor<T> {

    T visit(CollectModifyOperation selectOperation);
}
