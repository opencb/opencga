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

public class RegenieStep1WrapperParams extends ToolParams {

    public static final String DESCRIPTION = "Regenie step1 parameters";

    @DataField(id = "phenoFile", description = FieldConstants.REGENIE_PHENO_FILE_DESCRIPTION)
    private String phenoFile;

    @DataField(id = "covarFile", description = FieldConstants.REGENIE_COVAR_FILE_DESCRIPTION)
    private String covarFile;

    @DataField(id = "dockerNamespace", description = FieldConstants.REGENIE_WALKER_DOCKER_NAMESPACE_DESCRIPTION)
    private String dockerNamespace;

    @DataField(id = "dockerUsername", description = FieldConstants.REGENIE_WALKER_DOCKER_USERNAME_DESCRIPTION)
    private String dockerUsername;

    @DataField(id = "dockerPassword", description = FieldConstants.REGENIE_WALKER_DOCKER_PASSWORD_DESCRIPTION)
    private String dockerPassword;

    public RegenieStep1WrapperParams() {
    }

    public RegenieStep1WrapperParams(String phenoFile, String covarFile, String dockerNamespace, String dockerUsername,
                                     String dockerPassword) {
        this.phenoFile = phenoFile;
        this.covarFile = covarFile;
        this.dockerNamespace = dockerNamespace;
        this.dockerUsername = dockerUsername;
        this.dockerPassword = dockerPassword;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieStep1WrapperParams{");
        sb.append("phenoFile='").append(phenoFile).append('\'');
        sb.append(", covarFile='").append(covarFile).append('\'');
        sb.append(", dockerNamespace='").append(dockerNamespace).append('\'');
        sb.append(", dockerUsername='").append(dockerUsername).append('\'');
        sb.append(", dockerPassword='").append(dockerPassword).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPhenoFile() {
        return phenoFile;
    }

    public RegenieStep1WrapperParams setPhenoFile(String phenoFile) {
        this.phenoFile = phenoFile;
        return this;
    }

    public String getCovarFile() {
        return covarFile;
    }

    public RegenieStep1WrapperParams setCovarFile(String covarFile) {
        this.covarFile = covarFile;
        return this;
    }

    public String getDockerNamespace() {
        return dockerNamespace;
    }

    public RegenieStep1WrapperParams setDockerNamespace(String dockerNamespace) {
        this.dockerNamespace = dockerNamespace;
        return this;
    }

    public String getDockerUsername() {
        return dockerUsername;
    }

    public RegenieStep1WrapperParams setDockerUsername(String dockerUsername) {
        this.dockerUsername = dockerUsername;
        return this;
    }

    public String getDockerPassword() {
        return dockerPassword;
    }

    public RegenieStep1WrapperParams setDockerPassword(String dockerPassword) {
        this.dockerPassword = dockerPassword;
        return this;
    }
}
