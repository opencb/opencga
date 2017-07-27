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
