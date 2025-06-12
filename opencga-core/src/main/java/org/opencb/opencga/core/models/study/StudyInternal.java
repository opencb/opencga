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

package org.opencb.opencga.core.models.study;

import org.opencb.biodata.models.common.Status;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.study.configuration.StudyConfiguration;

public class StudyInternal extends Internal {


    @DataField(id = "status", indexed = true,
            description = FieldConstants.GENERIC_STATUS_DESCRIPTION)
    private Status status;

    @DataField(id = "index", indexed = true,
            description = FieldConstants.STUDY_INTERNAL_INDEX)
    private StudyIndex index;

    @DataField(id = "configuration", indexed = true, uncommentedClasses = {"StudyConfiguration"},
            description = FieldConstants.STUDY_INTERNAL_CONFIGURATION)
    private StudyConfiguration configuration;

    @DataField(id = "federated", indexed = true, description = FieldConstants.STUDY_INTERNAL_FEDERATED)
    private boolean federated;

    public StudyInternal() {
    }

    public StudyInternal(InternalStatus status, String registrationDate, String modificationDate, StudyIndex index,
                         StudyConfiguration configuration, boolean federated) {
        super(status, registrationDate, modificationDate);
        this.index = index;
        this.configuration = configuration;
        this.federated = false;
    }

    public static StudyInternal init(String cellbaseVersion) {
        return new StudyInternal(new InternalStatus(), TimeUtils.getTime(), TimeUtils.getTime(), StudyIndex.init(),
                StudyConfiguration.init(cellbaseVersion), false);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyInternal{");
        sb.append("registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(lastModified).append('\'');
        sb.append(", status=").append(status);
        sb.append(", index=").append(index);
        sb.append(", configuration=").append(configuration);
        sb.append(", federated=").append(federated);
        sb.append('}');
        return sb.toString();
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public StudyInternal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public StudyInternal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public StudyIndex getIndex() {
        return index;
    }

    public StudyInternal setIndex(StudyIndex index) {
        this.index = index;
        return this;
    }

    public StudyConfiguration getConfiguration() {
        return configuration;
    }

    public StudyInternal setConfiguration(StudyConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    public boolean isFederated() {
        return federated;
    }

    public void setFederated(boolean federated) {
        this.federated = federated;
    }
}
