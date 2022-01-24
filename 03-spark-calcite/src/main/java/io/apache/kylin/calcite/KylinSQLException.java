package io.apache.kylin.calcite;

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
}
