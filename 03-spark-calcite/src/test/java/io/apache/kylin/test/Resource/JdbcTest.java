package io.apache.kylin.test.Resource;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.test.AbstractModifiableTable;
import org.apache.calcite.test.schemata.hr.Department;
import org.apache.calcite.test.schemata.hr.Employee;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.util.TryThreadLocal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JdbcTest {
    static AbstractModifiableTable mutable(String tableName,
                                           final List<Employee> employees) {
        return new AbstractModifiableTable(tableName) {
            public RelDataType getRowType(
                    RelDataTypeFactory typeFactory) {
                return ((JavaTypeFactory) typeFactory)
                        .createType(Employee.class);
            }

            public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                                SchemaPlus schema, String tableName) {
                return new AbstractTableQueryable<T>(queryProvider, schema, this,
                        tableName) {
                    public Enumerator<T> enumerator() {
                        //noinspection unchecked
                        return (Enumerator<T>) Linq4j.enumerator(employees);
                    }
                };
            }

            public Type getElementType() {
                return Employee.class;
            }

            public Expression getExpression(SchemaPlus schema, String tableName,
                                            Class clazz) {
                return Schemas.tableExpression(schema, getElementType(), tableName,
                        clazz);
            }

            public Collection getModifiableCollection() {
                return employees;
            }
        };
    }

    /** Factory for EMP and DEPT tables. */
    public static class EmpDeptTableFactory implements TableFactory<Table> {
        public static final TryThreadLocal<List<Employee>> THREAD_COLLECTION =
                TryThreadLocal.of(null);

        public Table create(
                SchemaPlus schema,
                String name,
                Map<String, Object> operand,
                @Nullable RelDataType rowType) {
            final Class clazz;
            final Object[] array;
            switch (name) {
                case "EMPLOYEES":
                    clazz = Employee.class;
                    array = new HrSchema().emps;
                    break;
                case "MUTABLE_EMPLOYEES":
                    List<Employee> employees = THREAD_COLLECTION.get();
                    if (employees == null) {
                        employees = Collections.emptyList();
                    }
                    return mutable(name, employees);
                case "DEPARTMENTS":
                    clazz = Department.class;
                    array = new HrSchema().depts;
                    break;
                default:
                    throw new AssertionError(name);
            }
            return new AbstractQueryableTable(clazz) {
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    return ((JavaTypeFactory) typeFactory).createType(clazz);
                }

                public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                                    SchemaPlus schema, String tableName) {
                    return new AbstractTableQueryable<T>(queryProvider, schema, this,
                            tableName) {
                        public Enumerator<T> enumerator() {
                            @SuppressWarnings("unchecked") final List<T> list =
                                    (List) Arrays.asList(array);
                            return Linq4j.enumerator(list);
                        }
                    };
                }
            };
        }
    }
}
