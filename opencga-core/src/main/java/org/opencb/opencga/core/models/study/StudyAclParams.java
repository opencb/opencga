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

package org.opencb.opencga.core.models.study;

import org.opencb.opencga.core.models.AclParams;

// Acl params to communicate the WS and the sample manager
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class StudyAclParams extends AclParams {

    @DataField(description = ParamConstants.STUDY_ACL_PARAMS_TEMPLATE_DESCRIPTION)
    private String template;

    public StudyAclParams() {
    }

    public StudyAclParams(String permissions, String template) {
        super(permissions);
        this.template = template;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyAclParams{");
        sb.append("permissions='").append(permissions).append('\'');
        sb.append(", template='").append(template).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getTemplate() {
        return template;
    }

    public StudyAclParams setTemplate(String template) {
        this.template = template;
        return this;
    }

    public StudyAclParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
