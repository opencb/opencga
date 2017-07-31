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

package org.opencb.opencga.storage.core.exceptions;

import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.List;

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

    public static StorageEngineException otherOperationInProgressException(BatchFileOperation opInProgress,
                                                                           BatchFileOperation currentOperation) {
        return otherOperationInProgressException(opInProgress, currentOperation.getOperationName(), currentOperation.getFileIds());
    }

    public static StorageEngineException otherOperationInProgressException(BatchFileOperation opInProgress,
                                                                           String currentOperationName, List<Integer> fileIds) {
        if (currentOperationName.equals(opInProgress.getOperationName()) && fileIds.equals(opInProgress.getFileIds())) {
            return currentOperationInProgressException(opInProgress);
        }
        return new StorageEngineException("Can not " + currentOperationName + " files " + fileIds
                + " while there is an operation \"" + opInProgress.getOperationName() + "\" "
                + "in status \"" + opInProgress.currentStatus() + "\" for files " + opInProgress.getFileIds() + ". "
                + "Finish or resume operations to continue.");
    }

    public static StorageEngineException currentOperationInProgressException(BatchFileOperation opInProgress) {
        return new StorageEngineException("Operation \"" + opInProgress.getOperationName() + "\" "
                + "for files " + opInProgress.getFileIds() + ' '
                + "in status \"" + opInProgress.currentStatus() + "\". "
                + "Relaunch with " + VariantStorageEngine.Options.RESUME.key() + "=true to finish the operation.");
    }


    public static StorageEngineException unableToExecute(String message, int fileId, String fileName) {
        return new StorageEngineException("Unable to perform action over file \"" + fileName + "\" (" + fileId + "). " + message);
    }

}
