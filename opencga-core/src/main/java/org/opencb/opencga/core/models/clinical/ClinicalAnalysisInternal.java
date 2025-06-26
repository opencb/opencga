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
import org.opencb.opencga.core.models.common.InternalStatus;

public class ClinicalAnalysisInternal extends Internal {

    private CvdbIndex cvdbIndex;

    public ClinicalAnalysisInternal() {
    }

    public ClinicalAnalysisInternal(InternalStatus status, String registrationDate, String lastModified, CvdbIndex cvdbIndex) {
        super(status, registrationDate, lastModified);
        this.cvdbIndex = cvdbIndex;
    }

    public static ClinicalAnalysisInternal init() {
        return new ClinicalAnalysisInternal(new InternalStatus(), TimeUtils.getTime(), TimeUtils.getTime(), CvdbIndex.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisInternal{");
        sb.append("cvdbIndex=").append(cvdbIndex);
        sb.append(", status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public ClinicalAnalysisInternal setStatus(InternalStatus status) {
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

    public CvdbIndex getCvdbIndex() {
        return cvdbIndex;
    }

    public ClinicalAnalysisInternal setCvdbIndex(CvdbIndex cvdbIndex) {
        this.cvdbIndex = cvdbIndex;
        return this;
    }
}
