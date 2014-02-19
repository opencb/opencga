package org.opencb.opencga.lib.auth;

import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;


/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class SqliteCredentials implements OpenCGACredentials {

    private final Path path;

    public SqliteCredentials(Path path) throws IllegalOpenCGACredentialsException {
        this.path = path;
        check();
    }
    
    public SqliteCredentials(Properties properties) throws IllegalOpenCGACredentialsException {
        this(Paths.get(properties.getProperty("db_path")));
    }

    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {

        try {
        FileUtils.checkPath(path.getParent(), true);
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
