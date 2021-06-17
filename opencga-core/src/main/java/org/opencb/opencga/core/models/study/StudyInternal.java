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

import org.opencb.opencga.core.models.common.Status;

public class StudyInternal {

    private Status status;
    private StudyVariantEngineConfiguration variantEngineConfiguration;

    public StudyInternal() {
    }

    public StudyInternal(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public StudyInternal setStatus(Status status) {
        this.status = status;
        return this;
    }

    public StudyVariantEngineConfiguration getVariantEngineConfiguration() {
        return variantEngineConfiguration;
    }

    public StudyInternal setVariantEngineConfiguration(StudyVariantEngineConfiguration variantEngineConfiguration) {
        this.variantEngineConfiguration = variantEngineConfiguration;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyInternal{");
        sb.append("status=").append(status);
        sb.append(", variantEngineConfiguration=").append(variantEngineConfiguration);
        sb.append('}');
        return sb.toString();
    }
}
