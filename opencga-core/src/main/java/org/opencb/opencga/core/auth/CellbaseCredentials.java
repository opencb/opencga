package org.opencb.opencga.core.auth;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class CellbaseCredentials implements OpenCGACredentials {

    private final String host;
    private final String species;
    private final String version;

    public CellbaseCredentials(String host, String species, String version) throws IllegalOpenCGACredentialsException {
        this.host = host;
        this.species = species;
        this.version = version;
        
        check();
    }
    
    
    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {
        if (host == null || host.length() == 0) {
            throw new IllegalOpenCGACredentialsException("CellBase hostname or address is not valid");
        }
        if (species == null || species.length() == 0) {
            throw new IllegalOpenCGACredentialsException("Species name is not valid");
        }
        if (version == null || version.length() == 0) {
            throw new IllegalOpenCGACredentialsException("Database version is not valid");
        }
        
        return true;
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
