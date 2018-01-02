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

import java.util.LinkedHashSet;
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

    public static StorageEngineException otherOperationInProgressException(BatchFileOperation operation, String jobOperationName,
                                                                           List<Integer> fileIds) {
        return otherOperationInProgressException(operation, jobOperationName, fileIds, false);
    }

    public static StorageEngineException otherOperationInProgressException(BatchFileOperation opInProgress, String currentOperationName,
                                                                           List<Integer> fileIds, boolean resume) {
        if (opInProgress.sameOperation(fileIds, opInProgress.getType(), currentOperationName)) {
            return currentOperationInProgressException(opInProgress);
        }
        if (resume && opInProgress.getOperationName().equals(currentOperationName)) {
            return new StorageEngineException("Can not resume \"" + currentOperationName + "\" "
                    + "in status \"" + opInProgress.currentStatus() + "\" with input files " + fileIds + ". "
                    + "Input files must be same from the previous batch: " + opInProgress.getFileIds());
        } else {
            return new StorageEngineException("Can not \"" + currentOperationName + "\" files " + fileIds
                    + " while there is an operation \"" + opInProgress.getOperationName() + "\" "
                    + "in status \"" + opInProgress.currentStatus() + "\" for files " + opInProgress.getFileIds() + ". "
                    + "Finish or resume operation to continue.");
        }
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

    public static StorageEngineException alreadyLoadedSamples(StudyConfiguration studyConfiguration, int fileId) {
        LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);

        StringBuilder sb = new StringBuilder();
        sb.append("Unable to load file '").append(studyConfiguration.getFileIds().inverse().get(fileId)).append("'. ");
        if (sampleIds != null && sampleIds.size() == 1) {
            sb.append("Sample '").append(studyConfiguration.getSamplesInFiles().get(fileId).iterator().next()).append("' is ");
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

    public static StorageEngineException alreadyLoadedSomeSamples(StudyConfiguration studyConfiguration, int fileId) {
        return new StorageEngineException(
                "Unable to load file '" + studyConfiguration.getFileIds().inverse().get(fileId) + "'. "
                        + "There was some already loaded samples, but not all of them.");
    }
}
