package io.apache.kylin.calcite.util;

import io.apache.kylin.calcite.KylinSQLException;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.SourceStringReader;

import java.io.Reader;

import static io.apache.kylin.calcite.impl.CalciteConfig.DEFAULT_PARSER_CONFIG;

@Deprecated
public class Commons {
   /** */
   private Commons() {
        // No-op.
   }

    /**
     * Parses a SQL statement.
     *
     * @param qry Query string.
     * @param parserCfg Parser config.
     * @return Parsed query.
     */
   public static SqlNodeList parse(String qry, SqlParser.Config parserCfg) {
       try {
            return parse(new SourceStringReader(qry), parserCfg);
       } catch (SqlParseException e) {
           throw new KylinSQLException("Failed to parse query.", e);
       }
   }

    /**
     * Parses a SQL statement.
     *
     * @param reader Source string reader.
     * @param parserCfg Parser config.
     * @return Parsed query.
     * @throws org.apache.calcite.sql.parser.SqlParseException on parse error.
     */
   public static SqlNodeList parse(Reader reader, SqlParser.Config parserCfg) throws SqlParseException {
       SqlParser parser = SqlParser.create(reader, parserCfg);
       return parser.parseStmtList();
   }

   public static SqlNodeList parse(String qry) {
       return parse(qry, DEFAULT_PARSER_CONFIG);
   }
}
