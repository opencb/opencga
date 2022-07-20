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

package org.opencb.opencga.core.models.individual;

import org.opencb.opencga.core.models.AclParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class IndividualAclUpdateParams extends AclParams {

    @DataField(description = ParamConstants.INDIVIDUAL_ACL_UPDATE_PARAMS_INDIVIDUAL_DESCRIPTION)
    private String individual;
    @DataField(description = ParamConstants.INDIVIDUAL_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;

    public IndividualAclUpdateParams() {
    }

    public IndividualAclUpdateParams(String individual, String sample, String permissions) {
        super(permissions);
        this.individual = individual;
        this.sample = sample;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualAclUpdateParams{");
        sb.append("individual='").append(individual).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getIndividual() {
        return individual;
    }

    public IndividualAclUpdateParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public IndividualAclUpdateParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public IndividualAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
