package org.opencb.opencga.clinical.rga.exceptions;

public class RgaException extends Exception {

    public RgaException(String message) {
        super(message);
    }

    public RgaException(String message, Throwable cause) {
        super(message, cause);
    }

}
