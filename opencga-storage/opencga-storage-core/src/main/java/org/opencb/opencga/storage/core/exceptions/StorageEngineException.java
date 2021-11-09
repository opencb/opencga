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

import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

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

    public static StorageEngineException otherOperationInProgressException(TaskMetadata operation, String jobOperationName,
                                                                           List<Integer> fileIds,
                                                                           VariantStorageMetadataManager mm) {
        return otherOperationInProgressException(operation, jobOperationName, fileIds, mm, false);
    }

    public static StorageEngineException otherOperationInProgressException(TaskMetadata opInProgress, String currentOperationName,
                                                                           List<Integer> fileIds,
                                                                           VariantStorageMetadataManager mm, boolean resume) {
        if (opInProgress.sameOperation(fileIds, opInProgress.getType(), currentOperationName)) {
            return currentOperationInProgressException(opInProgress, mm);
        }
        if (resume && opInProgress.getName().equals(currentOperationName)) {
            return new StorageEngineException("Can not resume \"" + currentOperationName + "\" "
                    + "in status \"" + opInProgress.currentStatus() + "\" with input files "
                    + fileIdsToString(mm, opInProgress.getStudyId(), fileIds) + ". "
                    + "Input files must be same from the previous batch: "
                    + fileIdsToString(mm, opInProgress.getStudyId(), opInProgress.getFileIds()));
        } else {
            return new StorageEngineException("Can not \"" + currentOperationName + "\" files "
                    + fileIdsToString(mm, opInProgress.getStudyId(), fileIds)
                    + " while there is an operation \"" + opInProgress.getName() + "\" "
                    + "in status \"" + opInProgress.currentStatus() + "\" for files "
                    + fileIdsToString(mm, opInProgress.getStudyId(), opInProgress.getFileIds()) + ". "
                    + "Finish or resume operation to continue.");
        }
    }

    public static StorageEngineException currentOperationInProgressException(TaskMetadata opInProgress, VariantStorageMetadataManager mm) {
        return new StorageEngineException("Operation \"" + opInProgress.getName() + "\" "
                + "for files " + fileIdsToString(mm, opInProgress.getStudyId(), opInProgress.getFileIds()) + ' '
                + "in status \"" + opInProgress.currentStatus() + "\". "
                + "Relaunch with " + VariantStorageOptions.RESUME.key() + "=true to finish the operation.");
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
        if (samples == null) {
            sb.append("Samples from this file are ");
        } else if (samples.size() == 1) {
            sb.append("Sample '").append(samples.get(0)).append("' is ");
        } else if (samples.size() < 30) {
            sb.append("Samples ").append(samples).append(" from this file are ");
        } else {
            sb.append(samples.size()).append(" samples from this file are ");
        }
        sb.append("already loaded. "
                + "This variant storage does not allow to load multiple files from the same sample in the same study. "
                + "If the variants of the new file does not overlap with the already loaded variants, "
                + "because they are from a different chromosome, region, or a different variant type, "
                + "repeat the operation with the option ")
                .append("-D").append(VariantStorageOptions.LOAD_SPLIT_DATA.key())
                .append("=[")
                .append(VariantStorageEngine.SplitData.CHROMOSOME).append(", ")
                .append(VariantStorageEngine.SplitData.REGION).append("] . ")
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

    protected static String fileIdsToString(VariantStorageMetadataManager metadataManager, int studyId, List<Integer> fileIds) {
        StringBuilder sb = new StringBuilder();
        for (Integer fileId : fileIds) {
            if (sb.length() == 0) {
                sb.append("[");
            } else {
                sb.append(", ");
            }
            String fileName;
            try {
                fileName = metadataManager.getFileName(studyId, fileId);
            } catch (Exception e) {
                fileName = "Error fetching file name " + e.getMessage();
            }
            sb.append("\"").append(fileName).append("\" (id=").append(fileId).append(")");
        }
        sb.append("]");
        return sb.toString();
    }
}
