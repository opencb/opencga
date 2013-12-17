package org.opencb.opencga.lib.auth;

/**
 * A set of credentials to access a data source, such as: user, 
 * password, hostname and port.
 * 
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface OpenCGACredentials {
    
    /**
     * Check that these are valid credentials, for example:
     * <ul>
     * <li>If the data source is a file, check it exists and is readable</li>
     * <li>If using a remote database, check the host and port values are valid</li>
     * </ul>
     * 
     * @return If all checks passed
     * @throws IllegalOpenCGACredentialsException If some check failed
     */
    boolean check() throws IllegalOpenCGACredentialsException;
    
    /**
     * Convert these credentials to a JSON representation
     * 
     * @return The string containing the JSON representation
     */
    String toJson();
    
}
