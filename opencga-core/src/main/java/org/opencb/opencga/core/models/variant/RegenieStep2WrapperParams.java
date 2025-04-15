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

package org.opencb.opencga.core.models.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class RegenieStep2WrapperParams extends ToolParams {

    public static final String DESCRIPTION = "Regenie step2 parameters";

    @DataField(id = "phenoFile", description = FieldConstants.REGENIE_PHENO_FILE_DESCRIPTION, required = true)
    private String phenoFile;

    @DataField(id = "covarFile", description = FieldConstants.REGENIE_COVAR_FILE_DESCRIPTION)
    private String covarFile;

    @DataField(id = "predPath", description = FieldConstants.REGENIE_PRED_PATH_DESCRIPTION, required = true)
    private String predPath;

    @DataField(id = "dockerUsername", description = FieldConstants.REGENIE_WALKER_DOCKER_USERNAME_DESCRIPTION, required = true)
    private String dockerUsername;

    @DataField(id = "dockerPassword", description = FieldConstants.REGENIE_WALKER_DOCKER_PASSWORD_DESCRIPTION, required = true)
    private String dockerPassword;

    //    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
//    private String outdir;

    public RegenieStep2WrapperParams() {
    }

    public RegenieStep2WrapperParams(String phenoFile, String covarFile, String predPath, String dockerUsername, String dockerPassword) {
        this.phenoFile = phenoFile;
        this.covarFile = covarFile;
        this.predPath = predPath;
        this.dockerUsername = dockerUsername;
        this.dockerPassword = dockerPassword;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieStep2WrapperParams{");
        sb.append("phenoFile='").append(phenoFile).append('\'');
        sb.append(", covarFile='").append(covarFile).append('\'');
        sb.append(", predPath='").append(predPath).append('\'');
        sb.append(", dockerUsername='").append(dockerUsername).append('\'');
        sb.append(", dockerPassword='").append(dockerPassword).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPhenoFile() {
        return phenoFile;
    }

    public RegenieStep2WrapperParams setPhenoFile(String phenoFile) {
        this.phenoFile = phenoFile;
        return this;
    }

    public String getCovarFile() {
        return covarFile;
    }

    public RegenieStep2WrapperParams setCovarFile(String covarFile) {
        this.covarFile = covarFile;
        return this;
    }

    public String getPredPath() {
        return predPath;
    }

    public RegenieStep2WrapperParams setPredPath(String predPath) {
        this.predPath = predPath;
        return this;
    }

    public String getDockerUsername() {
        return dockerUsername;
    }

    public RegenieStep2WrapperParams setDockerUsername(String dockerUsername) {
        this.dockerUsername = dockerUsername;
        return this;
    }

    public String getDockerPassword() {
        return dockerPassword;
    }

    public RegenieStep2WrapperParams setDockerPassword(String dockerPassword) {
        this.dockerPassword = dockerPassword;
        return this;
    }
}
