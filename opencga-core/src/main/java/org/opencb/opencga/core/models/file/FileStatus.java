/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.Status;

import java.util.Arrays;
import java.util.List;

public class FileStatus extends Status {

    /**
     * TRASHED name means that the object is marked as deleted although is still available in the database.
     */
    public static final String TRASHED = "TRASHED";

    public static final String STAGE = "STAGE";
    public static final String MISSING = "MISSING";
    public static final String PENDING_DELETE = "PENDING_DELETE";
    public static final String DELETING = "DELETING"; // This status is set exactly before deleting the file from disk.
    public static final String REMOVED = "REMOVED";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, TRASHED, STAGE, MISSING, PENDING_DELETE, DELETING,
            REMOVED);

    public FileStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public FileStatus(String status) {
        this(status, "");
    }

    public FileStatus() {
        this(READY, "");
    }

    public static boolean isValid(String status) {
        if (Status.isValid(status)) {
            return true;
        }
        if (status != null && (status.equals(STAGE) || status.equals(MISSING) || status.equals(TRASHED)
                || status.equals(PENDING_DELETE) || status.equals(DELETING) || status.equals(REMOVED))) {
            return true;
        }
        return false;
    }
}
