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

import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

/**
 * Created by jacobo on 1/02/15.
 */
public class StorageEngineException extends Exception {

    public StorageEngineException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageEngineException(String message) {
        super(message);
    }


    public static StorageEngineException alreadyLoaded(int fileId, StudyConfiguration sc) {
        return unableToExecute("Already loaded", fileId, sc);
    }

    public static StorageEngineException alreadyLoaded(int fileId, String fileName) {
        return unableToExecute("Already loaded", fileId, fileName);
    }

    public static StorageEngineException unableToExecute(String action, int fileId, StudyConfiguration sc) {
        return unableToExecute(action, fileId, sc.getFileIds().inverse().get(fileId));
    }

    public static StorageEngineException unableToExecute(String message, int fileId, String fileName) {
        return new StorageEngineException("Unable to perform action over file \"" + fileName + "\" (" + fileId + "). " + message);
    }
}
