package org.opencb.opencga.core.exception;

/**
 * Created by pfurio on 23/05/17.
 */
public class ToolException extends Exception {

    public ToolException(String message) {
        super(message);
    }

    public ToolException(String message, Throwable cause) {
        super(message, cause);
    }

    public ToolException(Throwable cause) {
        super(cause);
    }

}
