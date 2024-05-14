package org.opencb.opencga.catalog.exceptions;

import org.apache.commons.lang3.StringUtils;

public class CatalogRuntimeException extends IllegalArgumentException {

    public CatalogRuntimeException(String message) {
        super(message);
    }

    public CatalogRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CatalogRuntimeException internalException(Exception e, String message) {
        if (e instanceof CatalogRuntimeException) {
            return ((CatalogRuntimeException) e);
        } else {
            if (StringUtils.isEmpty(message)) {
                message = e.getMessage();
                if (StringUtils.isEmpty(message)) {
                    message = e.toString();
                }
            }
            return new CatalogRuntimeException("Internal exception: " + message, e);
        }
    }

    public static CatalogRuntimeException internalException(Exception e) {
        return internalException(e, "");
    }


}
