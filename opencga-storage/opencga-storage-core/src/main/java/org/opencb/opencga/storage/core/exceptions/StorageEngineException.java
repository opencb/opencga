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

import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.io.IOException;
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

    public static StorageEngineException alreadyLoaded(int fileId, String fileName) {
        return unableToExecute("Already loaded", fileId, fileName);
    }

    public static StorageEngineException otherOperationInProgressException(TaskMetadata opInProgress,
                                                                           TaskMetadata currentOperation) {
        return otherOperationInProgressException(opInProgress, currentOperation.getName(), currentOperation.getFileIds());
    }

    public static StorageEngineException otherOperationInProgressException(TaskMetadata operation, String jobOperationName,
                                                                           List<Integer> fileIds) {
        return otherOperationInProgressException(operation, jobOperationName, fileIds, false);
    }

    public static StorageEngineException otherOperationInProgressException(TaskMetadata opInProgress, String currentOperationName,
                                                                           List<Integer> fileIds, boolean resume) {
        if (opInProgress.sameOperation(fileIds, opInProgress.getType(), currentOperationName)) {
            return currentOperationInProgressException(opInProgress);
        }
        if (resume && opInProgress.getName().equals(currentOperationName)) {
            return new StorageEngineException("Can not resume \"" + currentOperationName + "\" "
                    + "in status \"" + opInProgress.currentStatus() + "\" with input files " + fileIds + ". "
                    + "Input files must be same from the previous batch: " + opInProgress.getFileIds());
        } else {
            return new StorageEngineException("Can not \"" + currentOperationName + "\" files " + fileIds
                    + " while there is an operation \"" + opInProgress.getName() + "\" "
                    + "in status \"" + opInProgress.currentStatus() + "\" for files " + opInProgress.getFileIds() + ". "
                    + "Finish or resume operation to continue.");
        }
    }

    public static StorageEngineException currentOperationInProgressException(TaskMetadata opInProgress) {
        return new StorageEngineException("Operation \"" + opInProgress.getName() + "\" "
                + "for files " + opInProgress.getFileIds() + ' '
                + "in status \"" + opInProgress.currentStatus() + "\". "
                + "Relaunch with " + VariantStorageEngine.Options.RESUME.key() + "=true to finish the operation.");
    }

    public static StorageEngineException unableToExecute(String message, int fileId, String fileName) {
        return new StorageEngineException("Unable to perform action over file \"" + fileName + "\" (" + fileId + "). " + message);
    }

    public static StorageEngineException invalidReleaseException(int release, int currentRelease) {
        return new StorageEngineException("Unable to load files with release '" + release + "' "
                + "when the current release is '" + currentRelease + "'.");
    }

    public static StorageEngineException alreadyLoadedSamples(String fileName, List<String> samples) {
        StringBuilder sb = new StringBuilder();
        sb.append("Unable to load file '").append(fileName).append("'. ");
        if (samples != null && samples.size() == 1) {
            sb.append("Sample '").append(samples.get(0)).append("' is ");
        } else {
            sb.append("The samples from this file are ");
        }
        sb.append("already loaded. "
                + "This variant storage does not allow to load multiple files from the same sample in the same study. "
                + "If the variants of the new file does not overlap with the already loaded variants, "
                + "because they are from a different chromosome, or a different variant type, repeat the operation with the option ")
                .append("-D").append(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key()).append("=true . ")
                .append("WARNING: Wrong usage of this option may cause a data corruption in the database!");
        return new StorageEngineException(sb.toString());
    }

    public static StorageEngineException alreadyLoadedSomeSamples(String fileName) {
        return new StorageEngineException(
                "Unable to load file '" + fileName + "'. "
                        + "There was some already loaded samples, but not all of them.");
    }

    public static StorageEngineException ioException(String file, IOException e) {
        return new StorageEngineException(file, e);
    }

    public static StorageEngineException ioException(IOException e) {
        return new StorageEngineException(e.getMessage(), e);
    }
}
