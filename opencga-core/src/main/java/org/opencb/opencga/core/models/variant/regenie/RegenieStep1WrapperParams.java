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

import java.util.Map;

public class RegenieStep1WrapperParams extends ToolParams {

    @DataField(id = "vcfFile", description = FieldConstants.REGENIE_VCF_FILE_DESCRIPTION)
    private String vcfFile;

    @DataField(id = "regenieParams", description = FieldConstants.REGENIE_OPTIONS_DESCRIPTION)
    private ObjectMap regenieParams;

    @DataField(id = "dockerParams", description = FieldConstants.REGENIE_WALKER_DOCKER_NAME_DESCRIPTION)
    private RegenieDockerParams docker;

    public RegenieStep1WrapperParams() {
    }

    public RegenieStep1WrapperParams(String vcfFile, ObjectMap regenieParams, RegenieDockerParams docker) {
        this.vcfFile = vcfFile;
        this.regenieParams = regenieParams;
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieStep1WrapperParams{");
        sb.append("vcfFile='").append(vcfFile).append('\'');
        sb.append(", regenieParams=").append(regenieParams);
        sb.append(", docker=").append(docker);
        sb.append('}');
        return sb.toString();
    }

    public String getVcfFile() {
        return vcfFile;
    }

    public RegenieStep1WrapperParams setVcfFile(String vcfFile) {
        this.vcfFile = vcfFile;
        return this;
    }

    public ObjectMap getRegenieParams() {
        return regenieParams;
    }

    public RegenieStep1WrapperParams setRegenieParams(ObjectMap regenieParams) {
        this.regenieParams = regenieParams;
        return this;
    }

    public RegenieDockerParams getDocker() {
        return docker;
    }

    public RegenieStep1WrapperParams setDocker(RegenieDockerParams docker) {
        this.docker = docker;
        return this;
    }
}
