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

package org.opencb.opencga.core.models.admin;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class UserImportParams {

    @DataField(description = ParamConstants.USER_IMPORT_PARAMS_AUTHENTICATION_ORIGIN_ID_DESCRIPTION)
    private String authenticationOriginId;
    @DataField(description = ParamConstants.USER_IMPORT_PARAMS_ID_DESCRIPTION)
    private List<String> id;
    @DataField(description = ParamConstants.USER_IMPORT_PARAMS_RESOURCE_TYPE_DESCRIPTION)
    private ResourceType resourceType;
    @DataField(description = ParamConstants.USER_IMPORT_PARAMS_STUDY_DESCRIPTION)
    private String study;
    @DataField(description = ParamConstants.USER_IMPORT_PARAMS_STUDY_GROUP_DESCRIPTION)
    private String studyGroup;

    public enum ResourceType {
        USER,
        GROUP,
        APPLICATION
    }

    public UserImportParams() {
    }

    public UserImportParams(String authenticationOriginId, List<String> id, ResourceType resourceType, String study, String studyGroup) {
        this.authenticationOriginId = authenticationOriginId;
        this.id = id;
        this.resourceType = resourceType;
        this.study = study;
        this.studyGroup = studyGroup;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserImportParams{");
        sb.append("authenticationOriginId='").append(authenticationOriginId).append('\'');
        sb.append(", id=").append(id);
        sb.append(", resourceType=").append(resourceType);
        sb.append(", study='").append(study).append('\'');
        sb.append(", studyGroup='").append(studyGroup).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAuthenticationOriginId() {
        return authenticationOriginId;
    }

    public UserImportParams setAuthenticationOriginId(String authenticationOriginId) {
        this.authenticationOriginId = authenticationOriginId;
        return this;
    }

    public List<String> getId() {
        return id;
    }

    public UserImportParams setId(List<String> id) {
        this.id = id;
        return this;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public UserImportParams setResourceType(ResourceType resourceType) {
        this.resourceType = resourceType;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public UserImportParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getStudyGroup() {
        return studyGroup;
    }

    public UserImportParams setStudyGroup(String studyGroup) {
        this.studyGroup = studyGroup;
        return this;
    }
}
