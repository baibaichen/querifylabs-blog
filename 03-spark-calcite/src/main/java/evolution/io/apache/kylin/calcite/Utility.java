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
package evolution.io.apache.kylin.calcite;

import io.apache.kylin.calcite.util.Commons;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import static java.util.Objects.requireNonNull;

public class Utility {
    private Utility() {}

    public static SqlNode parse(CalciteConnectionConfig connectionConfig, String sql)  {
        SqlParser.Config config1 = SqlParser.config()
          .withCaseSensitive(connectionConfig.caseSensitive())
          .withUnquotedCasing(connectionConfig.unquotedCasing())
          .withQuotedCasing(connectionConfig.quotedCasing())
          .withConformance(connectionConfig.conformance());

        return Commons.parse(sql, config1);
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (;;) {
            SchemaPlus parentSchema = schema.getParentSchema();
            if (parentSchema == null) {
                return schema;
            }
            schema = parentSchema;
        }
    }

    /**
     * CalciteCatalogReader is stateless; just create one
     */
    public static CalciteCatalogReader createCatalogReader(
      CalciteConnectionConfig connectionConfig,
      SchemaPlus defaultSchema,
      RelDataTypeFactory typeFactory) {
        requireNonNull(defaultSchema, "defaultSchema");
        requireNonNull(typeFactory, "typeFactory");
        final SchemaPlus rootSchema = rootSchema(defaultSchema);

        return new CalciteCatalogReader(
          CalciteSchema.from(rootSchema),
          CalciteSchema.from(defaultSchema).path(null),
          typeFactory,
          connectionConfig);
    }

    public static SqlValidator createSqlValidator(
      CalciteConnectionConfig connectionConfig,
      Prepare.CatalogReader catalogReader,
      RelDataTypeFactory typeFactory) {
        // less flexible
        final SqlOperatorTable opTab =
          SqlOperatorTables.chain(SqlStdOperatorTable.instance(), catalogReader);

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
          .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
          .withSqlConformance(connectionConfig.conformance())
          .withDefaultNullCollation(connectionConfig.defaultNullCollation())
          .withIdentifierExpansion(true);

        return SqlValidatorUtil.newValidator(opTab, catalogReader, typeFactory, validatorConfig);
    }
}
