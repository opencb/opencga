package org.opencb.opencga.core.auth;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class IllegalOpenCGACredentialsException extends Exception {

    public IllegalOpenCGACredentialsException() {
    }
    
    public IllegalOpenCGACredentialsException(String message) {
        super(message);
    }

    public IllegalOpenCGACredentialsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public IllegalOpenCGACredentialsException(Throwable cause) {
        super(cause);
    }

    public IllegalOpenCGACredentialsException(String message, Throwable cause) {
        super(message, cause);
    }
    
    
}
