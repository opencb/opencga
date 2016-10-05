/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.exceptions;

import org.opencb.opencga.storage.core.StorageETLResult;

import java.util.List;

/**
 * Exception during the ETL pipeline.
 *
 * Includes a list of {@link StorageETLResult} with the execution
 * details, where at least one of them has failed.
 *
 * Created on 04/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StorageETLException extends StorageManagerException {

    private final List<StorageETLResult> results;

    public StorageETLException(String message, Throwable cause, List<StorageETLResult> results) {
        super(message, cause);
        this.results = results;
    }

    public StorageETLException(String message, List<StorageETLResult> results) {
        super(message);
        this.results = results;
    }

    public List<StorageETLResult> getResults() {
        return results;
    }
}
