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
package io.apache.kylin.calcite.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

/**
 * Integrated with Spark catalog.
 *
 * <p>Represents a wrapper for {@link org.apache.spark.sql.connector.catalog.Table Spark Table} in
 * {@link org.apache.calcite.schema.Schema caclite schema}. This table would be converted to {@link
 * io.apache.kylin.calcite.RelOptKylinTable} based on its internal source type.
 *
 * <P> TODO: is this wrapper really needed?
 */
public class SparkTable extends AbstractTable  {

    private final String viewText;
    private final List<RelDataType> columnTypes;
    private final List<String> columnNames;

    public SparkTable(String viewText,
                      List<RelDataType> columnTypes,
                      List<String> columnNames) {
        this.viewText = viewText;
        this.columnTypes = columnTypes;
        this.columnNames = columnNames;
    }

    /**
     * Calcite Type-inference strategy:
     * <ol>
     * <li> Count(*) => {@link org.apache.calcite.sql.fun.SqlCountAggFunction#deriveType deriveType} </li>
     * <li>  Sum  => {@link org.apache.calcite.sql.type.ReturnTypes#AGG_SUM } </li>
     * </ol>
     * @param typeFactory
     * @return
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(columnTypes, columnNames);
    }

    String getViewText(){
        return viewText;
    }
}
