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

package org.opencb.opencga.core.models.common;

import java.util.Arrays;
import java.util.List;

public class QualityControlStatus extends InternalStatus {

    /*
     * States
     *
     * NONE --> COMPUTING --> READY
     *                    --> INCOMPLETE
     */
    public static final String NONE = "NONE";
    public static final String COMPUTING = "COMPUTING";
    public static final String INCOMPLETE = "INCOMPLETE";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, NONE, COMPUTING, INCOMPLETE);

    public QualityControlStatus(String status, String message) {
        if (isValid(status)) {
            init(status, status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public QualityControlStatus(String status) {
        this(status, "");
    }

    public QualityControlStatus() {
        this(NONE, "");
    }

    @Override
    public String getId() {
        return super.getId();
    }

    @Override
    public String getName() {
        return super.getName();
    }

    public static IndexStatus init() {
        return new IndexStatus();
    }

    public static boolean isValid(String status) {
        return status != null
                && (status.equals(READY)
                || status.equals(DELETED)
                || status.equals(INCOMPLETE)
                || status.equals(NONE)
                || status.equals(COMPUTING));
    }
}
