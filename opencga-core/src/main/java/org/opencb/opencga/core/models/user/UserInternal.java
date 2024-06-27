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

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;

public class UserInternal extends Internal {

    private UserStatus status;
    private int failedAttempts;

    public UserInternal() {
    }

    public UserInternal(UserStatus status) {
        this(TimeUtils.getTime(), TimeUtils.getTime(), status);
    }

    public UserInternal(String registrationDate, String lastModified, UserStatus status) {
        super(null, registrationDate, lastModified);
        this.status = status;
        this.failedAttempts = 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserInternal{");
        sb.append("status=").append(status);
        sb.append(", failedAttempts=").append(failedAttempts);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public UserStatus getStatus() {
        return status;
    }

    public UserInternal setStatus(UserStatus status) {
        this.status = status;
        return this;
    }

    public int getFailedAttempts() {
        return failedAttempts;
    }

    public UserInternal setFailedAttempts(int failedAttempts) {
        this.failedAttempts = failedAttempts;
        return this;
    }
}
