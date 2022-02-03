package io.apache.kylin.pp.calcite;

import org.apache.calcite.util.Util;

public class KylinSQLException extends RuntimeException {

    public KylinSQLException() {
    }

    public KylinSQLException(String message) {
        super(message);
    }

    public KylinSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public KylinSQLException(Throwable cause) {
        super(cause);
    }

    public static KylinSQLException error(int code, String message) {
        Util.discard(code);
        return new KylinSQLException(message);
    }

    public static KylinSQLException error(int code, Throwable cause) {
        Util.discard(code);
        return new KylinSQLException(cause);
    }

    public static class ErrorCode {

        /** Generic parsing error. */
        public static final int PARSING = -1;

        /** Generic validating error. */
        public static final int VALIDATING = -2;
    }
}
