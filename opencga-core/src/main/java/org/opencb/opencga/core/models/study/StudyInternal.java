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

import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.study.configuration.StudyConfiguration;

public class StudyInternal extends Internal {

    private StudyConfiguration configuration;

    public StudyInternal() {
    }

    public StudyInternal(Status status, String registrationDate, StudyConfiguration configuration) {
        super(status, registrationDate);
        this.configuration = configuration;
    }

    public static StudyInternal init() {
        return new StudyInternal(new Status(), TimeUtils.getTime(), StudyConfiguration.init());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyInternal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public StudyInternal setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public Internal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public StudyConfiguration getConfiguration() {
        return configuration;
    }

    public StudyInternal setConfiguration(StudyConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
