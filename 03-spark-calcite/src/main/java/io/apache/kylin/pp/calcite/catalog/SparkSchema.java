package io.apache.kylin.pp.calcite.catalog;

import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/** Base class for spark {@link Schema}, which provides some default implementations. */
public abstract class SparkSchema implements Schema {
    @Override
    public RelProtoDataType getType(String name) {
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        return Collections.emptySet();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return Collections.emptyList();
    }

    @Override
    public Set<String> getFunctionNames() {
        return Collections.emptySet();
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
