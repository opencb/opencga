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

package org.opencb.opencga.core.models.variant.regenie;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class RegenieStep2WrapperParams extends ToolParams {

    @DataField(id = "step1JobId", description = FieldConstants.REGENIE_STEP1_JOB_ID_DESCRIPTION)
    private String step1JobId;

    @DataField(id = "dockerParams", description = FieldConstants.REGENIE_WALKER_DOCKER_NAME_DESCRIPTION)
    private RegenieDockerParams docker;

    public RegenieStep2WrapperParams() {
    }

    public RegenieStep2WrapperParams(String step1JobId, RegenieDockerParams docker) {
        this.step1JobId = step1JobId;
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieStep2WrapperParams{");
        sb.append("step1JobId='").append(step1JobId).append('\'');
        sb.append(", docker=").append(docker);
        sb.append('}');
        return sb.toString();
    }

    public String getStep1JobId() {
        return step1JobId;
    }

    public RegenieStep2WrapperParams setStep1JobId(String step1JobId) {
        this.step1JobId = step1JobId;
        return this;
    }

    public RegenieDockerParams getDocker() {
        return docker;
    }

    public RegenieStep2WrapperParams setDocker(RegenieDockerParams docker) {
        this.docker = docker;
        return this;
    }
}
