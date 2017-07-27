package org.opencb.opencga.catalog.exceptions;

/**
 * Created by pfurio on 26/07/17.
 */
public class CatalogAuthenticationException extends CatalogException {

    public CatalogAuthenticationException(String message) {
        super(message);
    }

    public CatalogAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogAuthenticationException(Throwable cause) {
        super(cause);
    }

    public static CatalogAuthenticationException tokenExpired(String token) {
        return new CatalogAuthenticationException("Authentication token is expired : " + token);
    }

    public static CatalogAuthenticationException invalidAuthenticationToken(String token) {
        return new CatalogAuthenticationException("Invalid authentication token : " + token);
    }

    public static CatalogAuthenticationException invalidAuthenticationEncodingToken(String token) {
        return new CatalogAuthenticationException("Invalid authentication token encoding : " + token);
    }

    public static CatalogAuthenticationException incorrectUserOrPassword() {
        return new CatalogAuthenticationException("Incorrect user or password.");
    }


}
