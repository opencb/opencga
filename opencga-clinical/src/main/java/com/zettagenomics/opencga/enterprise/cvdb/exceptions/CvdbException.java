package com.zettagenomics.opencga.enterprise.cvdb.exceptions;

public class CvdbException extends Exception {

    public final static String NO_RESULTS_FOUND = "No results found matching the query";

    public CvdbException(String message) {
        super(message);
    }

    public CvdbException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CvdbException noResultsMatching() {
        return new CvdbException(NO_RESULTS_FOUND);
    }
}
