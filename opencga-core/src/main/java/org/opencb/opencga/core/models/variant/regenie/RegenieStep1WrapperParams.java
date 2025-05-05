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

public class RegenieStep1WrapperParams extends ToolParams {

    @DataField(id = "phenoFile", description = FieldConstants.REGENIE_PHENO_FILE_DESCRIPTION)
    private String phenoFile;

    @DataField(id = "variantExportQuery", description = FieldConstants.REGENIE_VARIANT_EXPORT_QUERY_DESCRIPTION)
    private ObjectMap variantExportQuery;

    @DataField(id = "dockerParams", description = FieldConstants.REGENIE_WALKER_DOCKER_NAME_DESCRIPTION)
    private RegenieDockerParams docker;

    public RegenieStep1WrapperParams() {
    }

    public RegenieStep1WrapperParams(String phenoFile, ObjectMap variantExportQuery, RegenieDockerParams docker) {
        this.phenoFile = phenoFile;
        this.variantExportQuery = variantExportQuery;
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieStep1WrapperParams{");
        sb.append("phenoFile='").append(phenoFile).append('\'');
        sb.append(", variantExportQuery=").append(variantExportQuery);
        sb.append(", docker=").append(docker);
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

    public ObjectMap getVariantExportQuery() {
        return variantExportQuery;
    }

    public RegenieStep1WrapperParams setVariantExportQuery(ObjectMap variantExportQuery) {
        this.variantExportQuery = variantExportQuery;
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
