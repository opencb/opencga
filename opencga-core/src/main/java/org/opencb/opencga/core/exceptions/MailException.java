package org.opencb.opencga.core.exceptions;

public class MailException extends Exception {

    public MailException(String message) {
        super(message);
    }

    public MailException(String message, Throwable cause) {
        super(message, cause);
    }

    public MailException(Throwable cause) {
        super(cause);
    }

}
