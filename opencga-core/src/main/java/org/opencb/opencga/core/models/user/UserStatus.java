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

package org.opencb.opencga.core.models.user;

import org.opencb.opencga.core.models.common.Status;

import java.util.Arrays;
import java.util.List;

public class UserStatus extends Status {

    public static final String BANNED = "BANNED";

    public static final List<String> STATUS_LIST = Arrays.asList(READY, DELETED, BANNED);

    public UserStatus(String status, String message) {
        if (isValid(status)) {
            init(status, message);
        } else {
            throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    public UserStatus(String status) {
        this(status, "");
    }

    public UserStatus() {
        this(READY, "");
    }

    public static boolean isValid(String status) {
        if (Status.isValid(status)) {
            return true;
        }
        if (status != null && (status.equals(BANNED))) {
            return true;
        }
        return false;
    }
}
