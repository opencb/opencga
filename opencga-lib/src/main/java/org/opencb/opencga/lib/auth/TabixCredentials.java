package org.opencb.opencga.lib.auth;

import java.io.IOException;
import java.nio.file.Path;
import org.opencb.commons.utils.FileUtils;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class TabixCredentials implements OpenCGACredentials {

    private final Path path;

    public TabixCredentials(Path path) throws IllegalOpenCGACredentialsException {
        this.path = path;
        
        check();
    }
    
    
    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {
        try {
            FileUtils.checkFile(path);
        } catch (IOException ex) {
            throw new IllegalOpenCGACredentialsException(ex);
        }
        
        return true;
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    public final Path getPath() {
        return path;
    }
    
}
