/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.auth;

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
