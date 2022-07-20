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

package org.opencb.opencga.core.models.sample;

import org.opencb.opencga.core.models.AclParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class SampleAclUpdateParams extends AclParams {

    @DataField(description = ParamConstants.SAMPLE_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;
    @DataField(description = ParamConstants.SAMPLE_ACL_UPDATE_PARAMS_INDIVIDUAL_DESCRIPTION)
    private String individual;
    @DataField(description = ParamConstants.SAMPLE_ACL_UPDATE_PARAMS_FAMILY_DESCRIPTION)
    private String family;
    @DataField(description = ParamConstants.SAMPLE_ACL_UPDATE_PARAMS_FILE_DESCRIPTION)
    private String file;
    @DataField(description = ParamConstants.SAMPLE_ACL_UPDATE_PARAMS_COHORT_DESCRIPTION)
    private String cohort;

    public SampleAclUpdateParams() {
    }

    public SampleAclUpdateParams(String sample, String individual, String family, String file, String cohort, String permissions) {
        super(permissions);
        this.sample = sample;
        this.individual = individual;
        this.family = family;
        this.file = file;
        this.cohort = cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleAclUpdateParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", individual='").append(individual).append('\'');
        sb.append(", family='").append(family).append('\'');
        sb.append(", file='").append(file).append('\'');
        sb.append(", cohort='").append(cohort).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public SampleAclUpdateParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public SampleAclUpdateParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public SampleAclUpdateParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getFile() {
        return file;
    }

    public SampleAclUpdateParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getCohort() {
        return cohort;
    }

    public SampleAclUpdateParams setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public SampleAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }
}
