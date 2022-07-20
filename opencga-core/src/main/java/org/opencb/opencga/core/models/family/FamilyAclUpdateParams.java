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

package org.opencb.opencga.core.models.family;

import org.opencb.opencga.core.models.AclParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FamilyAclUpdateParams extends AclParams {

    @DataField(description = ParamConstants.FAMILY_ACL_UPDATE_PARAMS_FAMILY_DESCRIPTION)
    private String family;
    @DataField(description = ParamConstants.FAMILY_ACL_UPDATE_PARAMS_INDIVIDUAL_DESCRIPTION)
    private String individual;
    @DataField(description = ParamConstants.FAMILY_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;

    public FamilyAclUpdateParams() {
    }

    public FamilyAclUpdateParams(String permissions, String family, String individual, String sample) {
        super(permissions);
        this.family = family;
        this.individual = individual;
        this.sample = sample;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilyAclUpdateParams{");
        sb.append("family='").append(family).append('\'');
        sb.append(", individual='").append(individual).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFamily() {
        return family;
    }

    public FamilyAclUpdateParams setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getIndividual() {
        return individual;
    }

    public FamilyAclUpdateParams setIndividual(String individual) {
        this.individual = individual;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public FamilyAclUpdateParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public FamilyAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
