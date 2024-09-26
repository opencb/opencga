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

package org.opencb.opencga.core.models.family;

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.QualityControlStatus;

public class FamilyInternal extends Internal {

    private FamilyStatus status;
    private QualityControlStatus qualityControlStatus;

    public FamilyInternal() {
    }

    public FamilyInternal(String registrationDate, String modificationDate, FamilyStatus status,
                          QualityControlStatus qualityControlStatus) {
        super(null, registrationDate, modificationDate);
        this.status = status;
        this.qualityControlStatus = qualityControlStatus;
    }

    public static FamilyInternal init() {
        String time = TimeUtils.getTime();
        return new FamilyInternal(time, time, new FamilyStatus(), new QualityControlStatus());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append(", status=").append(status);
        sb.append(", qualityControlStatus=").append(qualityControlStatus);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public FamilyStatus getStatus() {
        return status;
    }

    public FamilyInternal setStatus(FamilyStatus status) {
        this.status = status;
        return this;
    }

    public QualityControlStatus getQualityControlStatus() {
        return qualityControlStatus;
    }

    public FamilyInternal setQualityControlStatus(QualityControlStatus qualityControlStatus) {
        this.qualityControlStatus = qualityControlStatus;
        return this;
    }
}
