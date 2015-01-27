package org.opencb.opencga.storage.core.variant.annotation;

/**
 * Created by jacobo on 27/01/15.
 */
public class VariantAnnotatorException extends Exception {

    public VariantAnnotatorException(String message) {
        super(message);
    }

    public VariantAnnotatorException(String message, Throwable cause) {
        super(message, cause);
    }
}
