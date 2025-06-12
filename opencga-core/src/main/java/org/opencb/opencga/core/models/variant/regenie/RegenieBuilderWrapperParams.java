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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class RegenieBuilderWrapperParams extends ToolParams {

    @DataField(id = "regenieFileOptions", description = FieldConstants.REGENIE_FILE_OPTIONS_DESCRIPTION)
    private ObjectMap regenieFileOptions;

    @DataField(id = "dockerParams", description = FieldConstants.REGENIE_WALKER_DOCKER_NAME_DESCRIPTION)
    private RegenieDockerParams docker;

    public RegenieBuilderWrapperParams() {
    }

    public RegenieBuilderWrapperParams(ObjectMap regenieFileOptions, RegenieDockerParams docker) {
        this.regenieFileOptions = regenieFileOptions;
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieBuilderWrapperParams{");
        sb.append("regenieFileOptions=").append(regenieFileOptions);
        sb.append(", docker=").append(docker);
        sb.append('}');
        return sb.toString();
    }

    public ObjectMap getRegenieFileOptions() {
        return regenieFileOptions;
    }

    public RegenieBuilderWrapperParams setRegenieFileOptions(ObjectMap regenieFileOptions) {
        this.regenieFileOptions = regenieFileOptions;
        return this;
    }

    public RegenieDockerParams getDocker() {
        return docker;
    }

    public RegenieBuilderWrapperParams setDocker(RegenieDockerParams docker) {
        this.docker = docker;
        return this;
    }
}
