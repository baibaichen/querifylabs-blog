/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package evolution.org.apache.kylin.meta;

import evolution.LatticeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.StarTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Virtual table that is composed of two or more tables joined together.
 *
 * <p>Model tables do not occur in end-user queries. They are introduced by the
 * optimizer to help matching queries to materializations, and used only
 * during the planning process.</p>
 *
 * <p>When a materialization is defined, if it involves a join, it is converted
 * to a query on top of a star table. Queries that are candidates to map onto
 * the materialization are mapped onto the same star table.</p>
 */
public class KylinModelTable
        extends AbstractTable {

    private final String viewText;
    private final @Nullable List<RelDataType> columnTypes;
    private final @Nullable List<String> columnNames;
    private final @Nullable StarTable interanlStarTable;

    public KylinModelTable(String viewText,
                           @Nullable List<RelDataType> columnTypes,
                           @Nullable List<String> columnNames,
                           @Nullable StarTable interanlStarTable) {
        this.viewText = viewText;
        if (interanlStarTable == null) {
            this.columnTypes = Objects.requireNonNull(columnTypes, "columnTypes");
            this.columnNames = Objects.requireNonNull(columnNames, "columnNames");
            this.interanlStarTable = null;
        } else {
            this.interanlStarTable = interanlStarTable;
            this.columnTypes = null;
            this.columnNames = null;
        }
    }

    /**
     * Calcite Type-inference strategy:
     * <ol>
     * <li> Count(*) => {@link org.apache.calcite.sql.fun.SqlCountAggFunction#deriveType deriveType} </li>
     * <li>  Sum  => {@link org.apache.calcite.sql.type.ReturnTypes#AGG_SUM } </li>
     * </ol>
     * @param typeFactory type factory
     * @return data type
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if(interanlStarTable == null) {
            return typeFactory.createStructType(columnTypes, columnNames);
        } else {
            return LatticeFactory.getFixRowType(typeFactory, interanlStarTable);
        }
    }

    String getViewText(){
        return viewText;
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        if (aClass.isInstance(interanlStarTable)) {
            return aClass.cast(interanlStarTable);
        } else {
            return super.unwrap(aClass);
        }
    }
}
