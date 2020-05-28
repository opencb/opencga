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

public class StudyAclUpdateParams extends AclParams {

    private String study;
    private String template;

    public StudyAclUpdateParams() {
    }

    public StudyAclUpdateParams(String permissions, String study, String template) {
        super(permissions);
        this.study = study;
        this.template = template;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyAclUpdateParams{");
        sb.append("study='").append(study).append('\'');
        sb.append(", template='").append(template).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStudy() {
        return study;
    }

    public StudyAclUpdateParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getTemplate() {
        return template;
    }

    public StudyAclUpdateParams setTemplate(String template) {
        this.template = template;
        return this;
    }

    public StudyAclUpdateParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
