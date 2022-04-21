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

package org.opencb.opencga.core.models.individual;

import org.opencb.biodata.models.common.Status;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;

public class IndividualInternal extends Internal {

    public IndividualInternal() {
    }

    public IndividualInternal(InternalStatus status, String registrationDate, String modificationDate) {
        super(status, registrationDate, modificationDate);
    }

    public static IndividualInternal init() {
        return new IndividualInternal(new InternalStatus(InternalStatus.READY), TimeUtils.getTime(), TimeUtils.getTime());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public IndividualInternal setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public IndividualInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public IndividualInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }

}
