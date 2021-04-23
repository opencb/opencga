package org.opencb.opencga.analysis.rga.exceptions;

public class RgaException extends Exception {

    public final static String NO_RESULTS_FOUND = "No results found matching the query";

    public RgaException(String message) {
        super(message);
    }

    public RgaException(String message, Throwable cause) {
        super(message, cause);
    }

    public static RgaException noResultsMatching() {
        return new RgaException(NO_RESULTS_FOUND);
    }

}
