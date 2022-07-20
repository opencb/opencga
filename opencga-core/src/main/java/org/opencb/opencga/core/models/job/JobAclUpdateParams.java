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

package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.AclParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class JobAclUpdateParams extends AclParams {

    @DataField(description = ParamConstants.JOB_ACL_UPDATE_PARAMS_JOB_DESCRIPTION)
    private String job;

    public JobAclUpdateParams() {
    }

    public JobAclUpdateParams(String job, String permissions) {
        super(permissions);
        this.job = job;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobAclUpdateParams{");
        sb.append("job='").append(job).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getJob() {
        return job;
    }

    public JobAclUpdateParams setJob(String job) {
        this.job = job;
        return this;
    }

    public JobAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
