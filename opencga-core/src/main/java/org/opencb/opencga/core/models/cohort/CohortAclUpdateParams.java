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

package org.opencb.opencga.core.models.cohort;

import org.opencb.opencga.core.models.AclParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CohortAclUpdateParams extends AclParams {

    @DataField(description = ParamConstants.COHORT_ACL_UPDATE_PARAMS_COHORT_DESCRIPTION)
    private String cohort;

    public CohortAclUpdateParams() {
    }

    public CohortAclUpdateParams(String permissions, String cohort) {
        super(permissions);
        this.cohort = cohort;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CohortAclUpdateParams{");
        sb.append("cohort='").append(cohort).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getCohort() {
        return cohort;
    }

    public CohortAclUpdateParams setCohort(String cohort) {
        this.cohort = cohort;
        return this;
    }

    public CohortAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
