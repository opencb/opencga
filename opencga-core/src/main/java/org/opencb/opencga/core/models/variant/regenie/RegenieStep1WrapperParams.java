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

    @DataField(id = "regenieParams", description = FieldConstants.REGENIE_OPTIONS_DESCRIPTION)
    private ObjectMap regenieParams;

    @DataField(id = "vcfFile", description = FieldConstants.REGENIE_VCF_FILE_DESCRIPTION)
    private String vcfFile;

    @DataField(id = "variantExportQuery", description = FieldConstants.REGENIE_VARIANT_EXPORT_QUERY_DESCRIPTION)
    private ObjectMap variantExportQuery;

    @DataField(id = "controlCohort", description = FieldConstants.REGENIE_CONTROL_COHORT_DESCRIPTION)
    private String controlCohort;

    @DataField(id = "caseCohort", description = FieldConstants.REGENIE_CASE_COHORT_DESCRIPTION)
    private String caseCohort;

    @DataField(id = "phenotype", description = FieldConstants.REGENIE_PHENOTYPE_DESCRIPTION)
    private String phenotype;

    @DataField(id = "dockerParams", description = FieldConstants.REGENIE_WALKER_DOCKER_NAME_DESCRIPTION)
    private RegenieDockerParams docker;

    public RegenieStep1WrapperParams() {
    }

    public RegenieStep1WrapperParams(ObjectMap regenieParams, String vcfFile, ObjectMap variantExportQuery, String controlCohort,
                                     String caseCohort, String phenotype, RegenieDockerParams docker) {
        this.regenieParams = regenieParams;
        this.vcfFile = vcfFile;
        this.variantExportQuery = variantExportQuery;
        this.controlCohort = controlCohort;
        this.caseCohort = caseCohort;
        this.phenotype = phenotype;
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieStep1WrapperParams{");
        sb.append("regenieParams=").append(regenieParams);
        sb.append(", vcfFile='").append(vcfFile).append('\'');
        sb.append(", variantExportQuery=").append(variantExportQuery);
        sb.append(", controlCohort='").append(controlCohort).append('\'');
        sb.append(", caseCohort='").append(caseCohort).append('\'');
        sb.append(", phenotype='").append(phenotype).append('\'');
        sb.append(", docker=").append(docker);
        sb.append('}');
        return sb.toString();
    }

    public ObjectMap getRegenieParams() {
        return regenieParams;
    }

    public RegenieStep1WrapperParams setRegenieParams(ObjectMap regenieParams) {
        this.regenieParams = regenieParams;
        return this;
    }

    public String getVcfFile() {
        return vcfFile;
    }

    public RegenieStep1WrapperParams setVcfFile(String vcfFile) {
        this.vcfFile = vcfFile;
        return this;
    }

    public ObjectMap getVariantExportQuery() {
        return variantExportQuery;
    }

    public RegenieStep1WrapperParams setVariantExportQuery(ObjectMap variantExportQuery) {
        this.variantExportQuery = variantExportQuery;
        return this;
    }

    public String getControlCohort() {
        return controlCohort;
    }

    public RegenieStep1WrapperParams setControlCohort(String controlCohort) {
        this.controlCohort = controlCohort;
        return this;
    }

    public String getCaseCohort() {
        return caseCohort;
    }

    public RegenieStep1WrapperParams setCaseCohort(String caseCohort) {
        this.caseCohort = caseCohort;
        return this;
    }

    public String getPhenotype() {
        return phenotype;
    }

    public RegenieStep1WrapperParams setPhenotype(String phenotype) {
        this.phenotype = phenotype;
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
