package evolution.org.apache.kylin.calcite.prepare;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.Context;

import java.util.Properties;

public class BaseQueryContext extends AbstractQueryContext{

    /** */
    public static final CalciteConnectionConfig CALCITE_CONNECTION_CONFIG;

    /**
     * @param parentCtx
     */
    public BaseQueryContext(Context parentCtx) {
        super(parentCtx);
    }

    static {
        Properties props = new Properties();

        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
          Boolean.FALSE.toString());

        props.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
          String.valueOf(true));

        props.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        props.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        CALCITE_CONNECTION_CONFIG = new CalciteConnectionConfigImpl(props);
    }
}
