package org.opencb.opencga.catalog.monitor.exceptions;

/**
 * Created by pfurio on 18/08/16.
 */
public class ExecutionException extends Exception {
    private static final long serialVersionUID = 1L;

    public ExecutionException(String msg) {
        super(msg);
    }

    public ExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ExecutionException(Throwable cause) {
        super(cause);
    }
}
