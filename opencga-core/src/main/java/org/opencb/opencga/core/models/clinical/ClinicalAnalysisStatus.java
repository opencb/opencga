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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalAnalysisStatus extends InternalStatus {

    public static final String INCOMPLETE = "INCOMPLETE";
    public static final String READY_FOR_VALIDATION = "READY_FOR_VALIDATION";
    public static final String READY_FOR_INTERPRETATION = "READY_FOR_INTERPRETATION";
    public static final String INTERPRETATION_IN_PROGRESS = "INTERPRETATION_IN_PROGRESS";
    //        public static final String INTERPRETED = "INTERPRETED";
    public static final String READY_FOR_INTEPRETATION_REVIEW = "READY_FOR_INTEPRETATION_REVIEW";
    public static final String INTERPRETATION_REVIEW_IN_PROGRESS = "INTERPRETATION_REVIEW_IN_PROGRESS";
    public static final String READY_FOR_REPORT = "READY_FOR_REPORT";
    public static final String REPORT_IN_PROGRESS = "REPORT_IN_PROGRESS";
    public static final String DONE = "DONE";
    public static final String REVIEW_IN_PROGRESS = "REVIEW_IN_PROGRESS";
    public static final String CLOSED = "CLOSED";
    public static final String REJECTED = "REJECTED";

    public static final List<String> STATUS_LIST = Arrays.asList(INCOMPLETE, READY, DELETED, READY_FOR_VALIDATION,
            READY_FOR_INTERPRETATION, INTERPRETATION_IN_PROGRESS, READY_FOR_INTEPRETATION_REVIEW, INTERPRETATION_REVIEW_IN_PROGRESS,
            READY_FOR_REPORT, REPORT_IN_PROGRESS, DONE, REVIEW_IN_PROGRESS, CLOSED, REJECTED);

    public ClinicalAnalysisStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public ClinicalAnalysisStatus(String status) {
        this(status, "");
    }

    public ClinicalAnalysisStatus() {
        this(READY_FOR_INTERPRETATION, "");
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
}
