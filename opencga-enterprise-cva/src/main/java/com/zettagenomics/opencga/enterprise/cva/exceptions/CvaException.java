package com.zettagenomics.opencga.enterprise.cva.exceptions;

public class CvaException extends Exception {

    public final static String NO_RESULTS_FOUND = "No results found matching the query";

    public CvaException(String message) {
        super(message);
    }

    public CvaException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CvaException noResultsMatching() {
        return new CvaException(NO_RESULTS_FOUND);
    }

}
