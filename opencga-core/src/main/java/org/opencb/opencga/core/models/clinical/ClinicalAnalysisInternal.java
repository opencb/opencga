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

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalAnalysisInternal extends Internal {

    @DataField(description = ParamConstants.GENERIC_STATUS_DESCRIPTION)
    private ClinicalAnalysisStatus status;

    public ClinicalAnalysisInternal() {
    }

    public ClinicalAnalysisInternal(String registrationDate, String modificationDate, ClinicalAnalysisStatus status) {
        super(null, registrationDate, modificationDate);
        this.status = status;
    }

    public static ClinicalAnalysisInternal init() {
        return new ClinicalAnalysisInternal(TimeUtils.getTime(), TimeUtils.getTime(), new ClinicalAnalysisStatus());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public ClinicalAnalysisStatus getStatus() {
        return status;
    }

    public ClinicalAnalysisInternal setStatus(ClinicalAnalysisStatus status) {
        this.status = status;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public ClinicalAnalysisInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public ClinicalAnalysisInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
