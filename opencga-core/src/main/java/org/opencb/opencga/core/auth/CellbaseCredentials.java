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
