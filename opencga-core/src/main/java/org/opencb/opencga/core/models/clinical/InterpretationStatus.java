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

package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.Arrays;
import java.util.List;

public class InterpretationStatus extends InternalStatus {

    public static final String NOT_REVIEWED = "NOT_REVIEWED";
    public static final String UNDER_REVIEW = "UNDER_REVIEW";
    public static final String REVIEWED = "REVIEWED";
    public static final String REJECTED = "REJECTED";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, NOT_REVIEWED, UNDER_REVIEW, REVIEWED, REJECTED);

    public InterpretationStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public InterpretationStatus() {
        this(NOT_REVIEWED, "");
    }

    public InterpretationStatus(String status) {
        this(status, "");
    }

    public static boolean isValid(String status) {
        if (InternalStatus.isValid(status)) {
            return true;
        }

        if (STATUS_LIST.contains(status)) {
            return true;
        }
        return false;
    }

    @Override
    public InterpretationStatus setId(String id) {
        super.setId(id);
        return this;
    }
}
