package org.apache.kylin.table.operations;

import java.util.Collections;

public final class CollectModifyOperation implements ModifyOperation {

    private final QueryOperation child;

    public CollectModifyOperation(QueryOperation child) {
        this.child = child;
    }

    @Override
    public QueryOperation getChild() {
        return child;
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String asSummaryString() {
        return OperationUtils.formatWithChildren(
                "CollectSink",
                Collections.emptyMap(),
                Collections.singletonList(child),
                Operation::asSummaryString);
    }
}
