package io.apache.kylin.test.Resource;

public class TPCHSchema {
    public static final String TPCH_01_NAME = "TPCH_01";
    public static final String TPCH_01_SCHEMA = "     {\n" +
            "       \"type\": \"custom\",\n" +
            "       \"name\": \"TPCH_01\",\n" +
            "       \"factory\": \"org.apache.calcite.adapter.tpch.TpchSchemaFactory\",\n" +
            "       \"operand\": {\n" +
            "         \"columnPrefix\": false,\n" +
            "         \"scale\": 0.01\n" +
            "       }\n" +
            "     }";


}
